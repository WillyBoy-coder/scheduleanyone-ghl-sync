const axios = require("axios");
const { DateTime } = require("luxon");

function splitName(fullName = "") {
  const parts = String(fullName).trim().split(/\s+/);
  const firstName = parts.shift() || "Unknown";
  const lastName = parts.join(" ");
  return { firstName, lastName };
}

function normalizePhone(phone) {
  if (!phone) return "";
  return String(phone).replace(/[^\d+]/g, "").trim();
}

function toNYISO(dateStr) {
  if (!dateStr || typeof dateStr !== "string") return null;

  const trimmed = dateStr.trim();
  const parts = trimmed.split(" ");
  if (parts.length !== 2) return null;

  const [datePart, timePart] = parts;
  const datePieces = datePart.split("/");
  const timePieces = timePart.split(":");

  if (datePieces.length !== 3 || timePieces.length !== 2) return null;

  const [year, month, day] = datePieces;
  const [hour, minute] = timePieces;

  const dt = DateTime.fromObject(
    {
      year: Number(year),
      month: Number(month),
      day: Number(day),
      hour: Number(hour),
      minute: Number(minute)
    },
    { zone: "America/New_York" }
  );

  return dt.isValid ? dt.toISO() : null;
}

function getWindow() {
  const now = DateTime.now().setZone("America/New_York");
  const lookaheadDays = Number(process.env.LOOKAHEAD_DAYS || 45);
  const end = now.plus({ days: lookaheadDays });

  return {
    start: now.toFormat("M/d/yyyy"),
    end: end.toFormat("M/d/yyyy")
  };
}

function isPastAppointment(startTime) {
  if (!startTime) return true;
  const now = DateTime.now().setZone("America/New_York");
  const appt = DateTime.fromISO(startTime).setZone("America/New_York");
  return appt < now;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withRetry(fn, label, maxAttempts = 4) {
  let attempt = 1;

  while (attempt <= maxAttempts) {
    try {
      return await fn();
    } catch (error) {
      const status = error.response?.status;

      if (status === 429 && attempt < maxAttempts) {
        const waitMs = attempt * 5000;
        console.log(`${label} hit 429. Waiting ${waitMs / 1000}s before retry...`);
        await sleep(waitMs);
        attempt++;
        continue;
      }

      throw error;
    }
  }
}

async function getScheduleAnyoneData(startDate, endDate) {
  const response = await axios.get(
    "https://www.membershipsalons.com/report/saledetailapi",
    {
      params: {
        apiToken: process.env.SA_TOKEN,
        start: startDate,
        end: endDate
      }
    }
  );

  return Array.isArray(response.data) ? response.data : [];
}

async function findExistingContactByPhone(phone) {
  if (!phone) return null;

  try {
    const response = await axios.get(
      `${process.env.GHL_BASE_URL}/contacts/search/duplicate`,
      {
        headers: {
          Authorization: `Bearer ${process.env.GHL_TOKEN}`,
          Version: "2021-07-28"
        },
        params: {
          locationId: process.env.GHL_LOCATION_ID,
          number: phone
        }
      }
    );

    return response.data?.contact || response.data || null;
  } catch {
    return null;
  }
}

async function createContact(record) {
  const { firstName, lastName } = splitName(record.Customer || "");
  const phone = normalizePhone(record.MobilePhone || "");

  const body = {
    locationId: process.env.GHL_LOCATION_ID,
    firstName,
    lastName,
    phone,
    source: "Schedule Anyone"
  };

  const response = await axios.post(
    `${process.env.GHL_BASE_URL}/contacts/`,
    body,
    {
      headers: {
        Authorization: `Bearer ${process.env.GHL_TOKEN}`,
        Version: "2021-07-28",
        "Content-Type": "application/json"
      }
    }
  );

  return response.data?.contact || response.data;
}

async function getOrCreateContact(record) {
  const phone = normalizePhone(record.MobilePhone || "");
  if (!phone) return null;

  const existing = await findExistingContactByPhone(phone);
  if (existing?.id) return existing;

  try {
    return await createContact(record);
  } catch (error) {
    const duplicateId = error.response?.data?.meta?.contactId;
    if (duplicateId) return { id: duplicateId };
    throw error;
  }
}

async function createAppointment(record, contactId, startTime, endTime) {
  const body = {
    calendarId: process.env.GHL_CALENDAR_ID,
    locationId: process.env.GHL_LOCATION_ID,
    contactId,
    startTime,
    endTime,
    title: record.ItemDescription || "Schedule Anyone Appointment",
    appointmentStatus: "confirmed",
    ignoreFreeSlotValidation: true,
    ignoreDateRange: true
  };

  const response = await axios.post(
    `${process.env.GHL_BASE_URL}/calendars/events/appointments`,
    body,
    {
      headers: {
        Authorization: `Bearer ${process.env.GHL_TOKEN}`,
        Version: "2021-04-15",
        "Content-Type": "application/json"
      }
    }
  );

  return response.data;
}

module.exports = async function handler(req, res) {
  try {
    const authHeader = req.headers.authorization;

    if (process.env.CRON_SECRET) {
      if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
        return res.status(401).json({
          ok: false,
          message: "Unauthorized"
        });
      }
    }

    const { start, end } = getWindow();
    console.log(`Checking Schedule Anyone from ${start} to ${end}`);

    const records = await withRetry(
      () => getScheduleAnyoneData(start, end),
      "Schedule Anyone fetch"
    );

    console.log(`Found ${records.length} records`);

    let created = 0;
    let skipped = 0;
    let errors = 0;

    for (const record of records) {
      const ticket = record.TicketNumber || "(no-ticket)";

      try {
        if (!record.TicketNumber) {
          skipped++;
          continue;
        }

        const startTime = toNYISO(record.StartDate);
        const endTime = toNYISO(record.EndDate);

        if (!startTime || !endTime) {
          console.log(`Skipping ${ticket}: invalid date`);
          skipped++;
          continue;
        }

        if (isPastAppointment(startTime)) {
          skipped++;
          continue;
        }

        const contact = await withRetry(
          () => getOrCreateContact(record),
          `Contact sync for ${ticket}`
        );

        if (!contact?.id) {
          console.log(`Skipping ${ticket}: no contact id`);
          skipped++;
          continue;
        }

        await withRetry(
          () => createAppointment(record, contact.id, startTime, endTime),
          `Create appointment for ${ticket}`
        );

        created++;
        console.log(`Created: ${ticket}`);

        await sleep(250);
      } catch (error) {
        errors++;
        console.log(`Error on ${ticket}:`, error.response?.data || error.message);
      }
    }

    return res.status(200).json({
      ok: true,
      window: { start, end },
      totalRecords: records.length,
      created,
      skipped,
      errors
    });
  } catch (error) {
    console.log("Cron failed:", error.response?.data || error.message);

    return res.status(500).json({
      ok: false,
      message: error.response?.data || error.message
    });
  }
};