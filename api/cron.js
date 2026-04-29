const axios = require("axios");
const { DateTime } = require("luxon");

const SA_API_URL =
  process.env.SA_API_URL ||
  "https://www.membershipsalons.com/report/saledetailapi";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizePhone(phone) {
  if (!phone) return "";
  return String(phone).replace(/[^\d+]/g, "").trim();
}

function normalizeTitle(title = "") {
  return String(title).trim().replace(/\s+/g, " ").toLowerCase();
}

function splitName(fullName = "") {
  const parts = String(fullName).trim().split(/\s+/);
  return {
    firstName: parts.shift() || "Unknown",
    lastName: parts.join(" ")
  };
}

function toNYISO(dateStr) {
  if (!dateStr || typeof dateStr !== "string") return null;

  const parts = dateStr.trim().split(" ");
  if (parts.length !== 2) return null;

  const [datePart, timePart] = parts;
  const [year, month, day] = datePart.split("/");
  const [hour, minute] = timePart.split(":");

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
  const start = DateTime.now().setZone("America/New_York").startOf("day");
  const lookaheadDays = Number(process.env.LOOKAHEAD_DAYS || 45);
  const end = start.plus({ days: lookaheadDays }).endOf("day");

  return { start, end };
}

function formatSADate(dt) {
  return dt.setZone("America/New_York").toFormat("M/d/yyyy");
}

function getDateChunks(start, end, chunkDays = 7) {
  const chunks = [];
  let cursor = start;

  while (cursor <= end) {
    const chunkEnd = DateTime.min(cursor.plus({ days: chunkDays - 1 }), end);
    chunks.push({ start: cursor, end: chunkEnd });
    cursor = chunkEnd.plus({ days: 1 }).startOf("day");
  }

  return chunks;
}

async function withRetry(fn, label, maxAttempts = 4) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (error) {
      if (error.response?.status === 429 && attempt < maxAttempts) {
        const waitMs = attempt * 5000;
        console.log(`${label} hit 429. Waiting ${waitMs / 1000}s...`);
        await sleep(waitMs);
        continue;
      }
      throw error;
    }
  }
}

async function getScheduleAnyoneDataForChunk(startDate, endDate) {
  const response = await axios.get(SA_API_URL, {
    params: {
      apiToken: process.env.SA_TOKEN,
      start: startDate,
      end: endDate
    }
  });

  return Array.isArray(response.data) ? response.data : [];
}

async function getAllScheduleAnyoneData(windowStart, windowEnd) {
  const chunks = getDateChunks(windowStart, windowEnd, 7);
  const allRecords = [];

  for (const chunk of chunks) {
    const start = formatSADate(chunk.start);
    const end = formatSADate(chunk.end);

    console.log(`Schedule Anyone chunk: ${start} → ${end}`);

    const records = await withRetry(
      () => getScheduleAnyoneDataForChunk(start, end),
      `Schedule Anyone fetch ${start}-${end}`
    );

    allRecords.push(...records);

    // Sequential execution + small delay
    await sleep(750);
  }

  return allRecords;
}

function groupByTicket(records) {
  const grouped = new Map();

  for (const record of records) {
    const ticket = record.TicketNumber;
    if (!ticket) continue;

    if (!grouped.has(ticket)) grouped.set(ticket, []);
    grouped.get(ticket).push(record);
  }

  return Array.from(grouped.entries()).map(([ticket, items]) => {
    const first = items[0];

    const descriptions = items
      .map((item) => item.ItemDescription)
      .filter(Boolean);

    return {
      ticket: String(ticket).trim(),
      customer: first.Customer || "",
      phone: first.MobilePhone || "",
      email: first.Email || "",
      staffName: first.StaffName || "",
      startDate: first.StartDate,
      endDate: first.EndDate,
      title:
        descriptions.join(" + ") ||
        first.ItemDescription ||
        "Schedule Anyone Appointment",
      rawItems: items
    };
  });
}

function buildGhlTitle(appointment) {
  return `[SA ${appointment.ticket}] ${appointment.title}`;
}

function isPastAppointment(startTime) {
  const now = DateTime.now().setZone("America/New_York");
  const appt = DateTime.fromISO(startTime).setZone("America/New_York");
  return appt < now;
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
  const { firstName, lastName } = splitName(appointment.customer);
  const phone = normalizePhone(appointment.phone);

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

async function getOrCreateContact(appointment) {
  const phone = normalizePhone(appointment.phone);
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

async function getCalendarEvents(windowStart, windowEnd) {
  const response = await axios.get(
    `${process.env.GHL_BASE_URL}/calendars/events`,
    {
      headers: {
        Authorization: `Bearer ${process.env.GHL_TOKEN}`,
        Version: "2021-04-15"
      },
      params: {
        calendarId: process.env.GHL_CALENDAR_ID,
        startTime: String(windowStart.toMillis()),
        endTime: String(windowEnd.toMillis())
      }
    }
  );

  return (
    response.data?.events ||
    response.data?.data ||
    response.data?.results ||
    []
  );
}

function getEventId(event) {
  return event.id || event.eventId || event.appointmentId || event._id || null;
}

function eventHasTicket(event, ticket) {
  const title = normalizeTitle(event.title || event.name || "");
  return title.includes(String(ticket).toLowerCase());
}

function eventMatchesLegacy(event, appointment, contactId, startTime, endTime) {
  const eventContactId =
    event.contactId || event.contact?.id || event.contact?.contactId || "";

  const eventStart = event.startTime || event.start || event.startAt || "";
  const eventEnd = event.endTime || event.end || event.endAt || "";

  const sameContact = String(eventContactId) === String(contactId);
  const sameStart =
    DateTime.fromISO(eventStart).toMillis() ===
    DateTime.fromISO(startTime).toMillis();
  const sameEnd =
    DateTime.fromISO(eventEnd).toMillis() ===
    DateTime.fromISO(endTime).toMillis();

  return sameContact && sameStart && sameEnd;
}

function findMatchingEvent(events, appointment, contactId, startTime, endTime) {
  return events.find((event) => {
    return (
      eventHasTicket(event, appointment.ticket) ||
      eventMatchesLegacy(event, appointment, contactId, startTime, endTime)
    );
  });
}

async function createAppointment(appointment, contactId, startTime, endTime) {
  const body = {
    calendarId: process.env.GHL_CALENDAR_ID,
    locationId: process.env.GHL_LOCATION_ID,
    contactId,
    startTime,
    endTime,
    title: buildGhlTitle(appointment),
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

async function updateAppointment(eventId, appointment, contactId, startTime, endTime) {
  const body = {
    calendarId: process.env.GHL_CALENDAR_ID,
    locationId: process.env.GHL_LOCATION_ID,
    contactId,
    startTime,
    endTime,
    title: buildGhlTitle(appointment),
    appointmentStatus: "confirmed",
    ignoreFreeSlotValidation: true,
    ignoreDateRange: true
  };

  await axios.put(
    `${process.env.GHL_BASE_URL}/calendars/events/appointments/${eventId}`,
    body,
    {
      headers: {
        Authorization: `Bearer ${process.env.GHL_TOKEN}`,
        Version: "2021-04-15",
        "Content-Type": "application/json"
      }
    }
  );
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

    console.log(
      `Checking Schedule Anyone from ${formatSADate(start)} to ${formatSADate(end)}`
    );

    const rawRecords = await getAllScheduleAnyoneData(start, end);
    const groupedAppointments = groupByTicket(rawRecords);

    console.log(`Raw records: ${rawRecords.length}`);
    console.log(`Grouped appointments: ${groupedAppointments.length}`);

    const existingEvents = await withRetry(
      () => getCalendarEvents(start, end),
      "Get GHL calendar events"
    );

    let created = 0;
    let updated = 0;
    let skipped = 0;
    let errors = 0;
    const details = [];

    for (const appointment of groupedAppointments) {
      const ticket = appointment.ticket;

      try {
        const startTime = toNYISO(appointment.startDate);
        const endTime = toNYISO(appointment.endDate);

        if (!startTime || !endTime) {
          skipped++;
          details.push({ ticket, reason: "invalid date" });
          continue;
        }

        if (isPastAppointment(startTime)) {
          skipped++;
          details.push({ ticket, reason: "past appointment", startTime });
          continue;
        }

        const contact = await withRetry(
          () => getOrCreateContact(appointment),
          `Contact sync for ${ticket}`
        );

        if (!contact?.id) {
          skipped++;
          details.push({ ticket, reason: "no contact id" });
          continue;
        }

        const match = findMatchingEvent(
          existingEvents,
          appointment,
          contact.id,
          startTime,
          endTime
        );

        if (match) {
          const eventId = getEventId(match);

          if (eventId) {
            await withRetry(
              () =>
                updateAppointment(
                  eventId,
                  appointment,
                  contact.id,
                  startTime,
                  endTime
                ),
              `Update appointment for ${ticket}`
            );

            updated++;
            details.push({ ticket, reason: "updated existing appointment" });
          } else {
            skipped++;
            details.push({ ticket, reason: "matched existing but no event id" });
          }

          continue;
        }

        await withRetry(
          () => createAppointment(appointment, contact.id, startTime, endTime),
          `Create appointment for ${ticket}`
        );

        created++;
        details.push({ ticket, reason: "created" });

        await sleep(250);
      } catch (error) {
        errors++;
        details.push({
          ticket,
          reason: "error",
          error: error.response?.data || error.message
        });
      }
    }

    return res.status(200).json({
      ok: true,
      window: {
        start: formatSADate(start),
        end: formatSADate(end)
      },
      rawRecordCount: rawRecords.length,
      groupedAppointmentCount: groupedAppointments.length,
      existingGhlEventCount: existingEvents.length,
      created,
      updated,
      skipped,
      errors,
      details
    });
  } catch (error) {
    return res.status(500).json({
      ok: false,
      message: error.response?.data || error.message
    });
  }
};