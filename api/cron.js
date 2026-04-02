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

function groupServiceRows(records) {
  const grouped = new Map();

  for (const record of records) {
    const ticket = record.TicketNumber;
    if (!ticket) continue;

    // Prefer rows that are real service rows
    if (record.Item !== "Service") continue;

    if (!grouped.has(ticket)) {
      grouped.set(ticket, record);
    } else {
      const existing = grouped.get(ticket);

      // Keep the earliest created/purchase row if multiple service rows exist
      const existingStart = toNYISO(existing.StartDate);
      const currentStart = toNYISO(record.StartDate);

      if (currentStart && existingStart && currentStart < existingStart) {
        grouped.set(ticket, record);
      }
    }
  }

  return Array.from(grouped.values());
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

async function searchContactByTicketNumber(ticketNumber) {
  if (!ticketNumber) return null;

  try {
    const response = await axios.post(
      `${process.env.GHL_BASE_URL}/contacts/search`,
      {
        locationId: process.env.GHL_LOCATION_ID,
        page: 1,
        pageLimit: 10,
        filters: [
          {
            field: "customFields.ticket_number",
            operator: "eq",
            value: ticketNumber
          }
        ]
      },
      {
        headers: {
          Authorization: `Bearer ${process.env.GHL_TOKEN}`,
          Version: "2021-07-28",
          "Content-Type": "application/json"
        }
      }
    );

    const contacts = response.data?.contacts || response.data?.data || [];
    return contacts[0] || null;
  } catch (error) {
    console.log("Ticket search failed:", error.response?.data || error.message);
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
    source: "Schedule Anyone",
    customFields: [
      {
        key: "ticket_number",
        field_value: record.TicketNumber || ""
      }
    ]
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

async function updateContactTicketNumber(contactId, ticketNumber) {
  if (!contactId || !ticketNumber) return null;

  const response = await axios.put(
    `${process.env.GHL_BASE_URL}/contacts/${contactId}`,
    {
      customFields: [
        {
          key: "ticket_number",
          field_value: ticketNumber
        }
      ]
    },
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
  const ticketMatch = await searchContactByTicketNumber(record.TicketNumber);
  if (ticketMatch?.id) return ticketMatch;

  const phone = normalizePhone(record.MobilePhone || "");
  if (!phone) return null;

  const existing = await findExistingContactByPhone(phone);
  if (existing?.id) {
    await updateContactTicketNumber(existing.id, record.TicketNumber);
    return existing;
  }

  try {
    return await createContact(record);
  } catch (error) {
    const duplicateId = error.response?.data?.meta?.contactId;
    if (duplicateId) {
      await updateContactTicketNumber(duplicateId, record.TicketNumber);
      return { id: duplicateId };
    }
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
    title: `${record.Customer || "Unknown"} - ${record.ItemDescription || "Schedule Anyone Appointment"}`,
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

    const rawRecords = await withRetry(
      () => getScheduleAnyoneData(start, end),
      "Schedule Anyone fetch"
    );

    const records = groupServiceRows(rawRecords);

    let created = 0;
    let skipped = 0;
    let errors = 0;
    const skippedDetails = [];

    for (const record of records) {
      const ticket = record.TicketNumber || "(no-ticket)";
      const customer = record.Customer || "(no-customer)";
      const phone = normalizePhone(record.MobilePhone || "");

      try {
        if (!record.TicketNumber) {
          skipped++;
          skippedDetails.push({ ticket, customer, reason: "missing TicketNumber" });
          continue;
        }

        const startTime = toNYISO(record.StartDate);
        const endTime = toNYISO(record.EndDate);

        if (!startTime || !endTime) {
          skipped++;
          skippedDetails.push({
            ticket,
            customer,
            reason: "invalid date",
            startDate: record.StartDate,
            endDate: record.EndDate
          });
          continue;
        }

        if (isPastAppointment(startTime)) {
          skipped++;
          skippedDetails.push({
            ticket,
            customer,
            reason: "past appointment",
            startTime
          });
          continue;
        }

        if (!phone) {
          skipped++;
          skippedDetails.push({
            ticket,
            customer,
            reason: "missing phone"
          });
          continue;
        }

        const existingByTicket = await searchContactByTicketNumber(record.TicketNumber);
        if (existingByTicket?.id) {
          skipped++;
          skippedDetails.push({
            ticket,
            customer,
            reason: "ticket already exists in GHL contact"
          });
          continue;
        }

        const contact = await withRetry(
          () => getOrCreateContact(record),
          `Contact sync for ${ticket}`
        );

        if (!contact?.id) {
          skipped++;
          skippedDetails.push({
            ticket,
            customer,
            reason: "no contact id returned"
          });
          continue;
        }

        await withRetry(
          () => createAppointment(record, contact.id, startTime, endTime),
          `Create appointment for ${ticket}`
        );

        created++;
      } catch (error) {
        errors++;
        skippedDetails.push({
          ticket,
          customer,
          reason: "exception",
          error: error.response?.data || error.message
        });
      }
    }

    return res.status(200).json({
      ok: true,
      window: { start, end },
      rawRecordCount: rawRecords.length,
      groupedServiceCount: records.length,
      created,
      skipped,
      errors,
      skippedDetails
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.response?.data || error.message
    });
  }
};