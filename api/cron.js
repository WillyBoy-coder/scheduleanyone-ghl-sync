const axios = require("axios");
const { DateTime } = require("luxon");

const TICKET_FIELD_KEY = "last_schedule_anyone_ticket";

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

function groupByTicket(records) {
  const grouped = new Map();

  for (const record of records) {
    const ticket = record.TicketNumber;
    if (!ticket) continue;

    if (!grouped.has(ticket)) {
      grouped.set(ticket, []);
    }

    grouped.get(ticket).push(record);
  }

  const groupedAppointments = [];

  for (const [ticket, items] of grouped.entries()) {
    const first = items[0];

    const descriptions = items
      .map((item) => item.ItemDescription)
      .filter(Boolean);

    groupedAppointments.push({
      ticket,
      customer: first.Customer || "",
      phone: first.MobilePhone || "",
      email: first.Email || "",
      staffName: first.StaffName || "",
      startDate: first.StartDate,
      endDate: first.EndDate,
      title: descriptions.join(" + ") || first.ItemDescription || "Schedule Anyone Appointment",
      rawItems: items
    });
  }

  return groupedAppointments;
}

function getCustomFieldValue(contact, fieldKey) {
  if (!contact || !fieldKey) return null;

  if (contact.customFields && Array.isArray(contact.customFields)) {
    const match = contact.customFields.find(
      (field) =>
        field.id === fieldKey ||
        field.key === fieldKey ||
        field.fieldKey === fieldKey
    );

    if (match) {
      return match.value ?? null;
    }
  }

  if (contact.customField && Array.isArray(contact.customField)) {
    const match = contact.customField.find(
      (field) =>
        field.id === fieldKey ||
        field.key === fieldKey ||
        field.fieldKey === fieldKey
    );

    if (match) {
      return match.value ?? null;
    }
  }

  if (contact[fieldKey] !== undefined) {
    return contact[fieldKey];
  }

  return null;
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

async function createContact(appointment) {
  const { firstName, lastName } = splitName(appointment.customer || "");
  const phone = normalizePhone(appointment.phone || "");

  const body = {
    locationId: process.env.GHL_LOCATION_ID,
    firstName,
    lastName,
    phone,
    email: appointment.email || undefined,
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

async function updateContactTicketField(contactId, ticket) {
  if (!contactId || !ticket || !TICKET_FIELD_KEY || TICKET_FIELD_KEY === "CUSTOM_FIELD_KEY_HERE") {
    return;
  }

  const body = {
    customFields: [
      {
        key: TICKET_FIELD_KEY,
        field_value: ticket
      }
    ]
  };

  await axios.put(
    `${process.env.GHL_BASE_URL}/contacts/${contactId}`,
    body,
    {
      headers: {
        Authorization: `Bearer ${process.env.GHL_TOKEN}`,
        Version: "2021-07-28",
        "Content-Type": "application/json"
      }
    }
  );
}

async function getOrCreateContact(appointment) {
  const phone = normalizePhone(appointment.phone || "");
  if (!phone) return null;

  const existing = await findExistingContactByPhone(phone);
  if (existing?.id) return existing;

  try {
    return await createContact(appointment);
  } catch (error) {
    const duplicateId = error.response?.data?.meta?.contactId;
    if (duplicateId) return { id: duplicateId };
    throw error;
  }
}

async function createAppointment(appointment, contactId, startTime, endTime) {
  const body = {
    calendarId: process.env.GHL_CALENDAR_ID,
    locationId: process.env.GHL_LOCATION_ID,
    contactId,
    startTime,
    endTime,
    title: appointment.title || "Schedule Anyone Appointment",
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

    const rawRecords = await withRetry(
      () => getScheduleAnyoneData(start, end),
      "Schedule Anyone fetch"
    );

    const groupedAppointments = groupByTicket(rawRecords);

    let created = 0;
    let skipped = 0;
    let errors = 0;
    const skippedDetails = [];

    for (const appointment of groupedAppointments) {
      const ticket = appointment.ticket;

      try {
        const startTime = toNYISO(appointment.startDate);
        const endTime = toNYISO(appointment.endDate);

        if (!startTime || !endTime) {
          skipped++;
          skippedDetails.push({
            ticket,
            customer: appointment.customer,
            reason: "invalid date"
          });
          continue;
        }

        if (isPastAppointment(startTime)) {
          skipped++;
          skippedDetails.push({
            ticket,
            customer: appointment.customer,
            reason: "past appointment",
            startTime
          });
          continue;
        }

        const contact = await withRetry(
          () => getOrCreateContact(appointment),
          `Contact sync for ${ticket}`
        );

        if (!contact?.id) {
          skipped++;
          skippedDetails.push({
            ticket,
            customer: appointment.customer,
            reason: "no contact id"
          });
          continue;
        }

        const existingTicket = getCustomFieldValue(contact, TICKET_FIELD_KEY);

        if (existingTicket && String(existingTicket).trim() === String(ticket).trim()) {
          skipped++;
          skippedDetails.push({
            ticket,
            customer: appointment.customer,
            reason: "duplicate ticket on contact"
          });
          continue;
        }

        await withRetry(
          () => createAppointment(appointment, contact.id, startTime, endTime),
          `Create appointment for ${ticket}`
        );

        await withRetry(
          () => updateContactTicketField(contact.id, ticket),
          `Update ticket field for ${ticket}`
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
      rawRecordCount: rawRecords.length,
      groupedServiceCount: groupedAppointments.length,
      created,
      skipped,
      errors,
      skippedDetails
    });
  } catch (error) {
    console.log("Cron failed:", error.response?.data || error.message);

    return res.status(500).json({
      ok: false,
      message: error.response?.data || error.message
    });
  }
};