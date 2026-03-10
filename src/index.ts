import { isAuthorized } from './lib/auth';
import {
  findEventByDedupeKey,
  insertInboundEvent,
  markEnqueued,
  markEnqueueing,
  recordEnqueueStatusError,
  updateDeliveryStatus
} from './lib/d1';
import { sendToHubSpot } from './lib/hubspot';
import { normalizeAffirmEvent } from './lib/normalize';
import { Env, QueueMessageBody } from './lib/types';

function jsonResponse(body: Record<string, unknown>, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' }
  });
}

function shouldRetryHubSpotStatus(statusCode: number): boolean {
  return statusCode === 429 || statusCode >= 500;
}

async function enqueueEvent(env: Env, message: QueueMessageBody): Promise<void> {
  await markEnqueueing(env.DB, message.eventId);
  await env.EVENT_QUEUE.send(message);
  try {
    await markEnqueued(env.DB, message.eventId);
  } catch (error) {
    const messageText =
      error instanceof Error ? `mark_enqueued_failed: ${error.message}` : 'mark_enqueued_failed';
    await recordEnqueueStatusError(env.DB, message.eventId, messageText);
  }
}

async function handleAffirmWebhook(request: Request, env: Env): Promise<Response> {
  if (!isAuthorized(request, env)) {
    return jsonResponse({ ok: false, error: 'unauthorized' }, 401);
  }

  const contentType = request.headers.get('content-type') || '';
  const rawBody = await request.text();

  let event: Awaited<ReturnType<typeof normalizeAffirmEvent>>;
  try {
    event = await normalizeAffirmEvent(rawBody, contentType, env.SOURCE_NAME);
  } catch {
    return jsonResponse({ ok: false, error: 'invalid_payload' }, 400);
  }

  const insertResult = await insertInboundEvent(env.DB, event);
  if (!insertResult.inserted) {
    const existing = await findEventByDedupeKey(env.DB, event.dedupeKey);

    if (existing && (existing.status === 'received' || existing.status === 'failed')) {
      try {
        await enqueueEvent(env, {
          eventId: existing.eventId,
          source: existing.source,
          eventType: existing.eventType,
          externalId: existing.externalId,
          dedupeKey: existing.dedupeKey,
          payload: existing.payload
        });

        return jsonResponse({ ok: true, duplicate: true, reenqueued: true, eventId: existing.eventId }, 202);
      } catch (error) {
        const messageText = error instanceof Error ? error.message : 'queue_enqueue_failed';
        await updateDeliveryStatus(env.DB, existing.eventId, 'failed', {
          lastError: messageText
        });
        return jsonResponse({ ok: false, error: 'queue_enqueue_failed' }, 503);
      }
    }

    return jsonResponse({ ok: true, duplicate: true, dedupeKey: event.dedupeKey }, 200);
  }

  try {
    await enqueueEvent(env, {
      eventId: event.eventId,
      source: event.source,
      eventType: event.eventType,
      externalId: event.externalId,
      dedupeKey: event.dedupeKey,
      payload: event.payload
    });
  } catch (error) {
    const messageText = error instanceof Error ? error.message : 'queue_enqueue_failed';
    await updateDeliveryStatus(env.DB, event.eventId, 'failed', {
      lastError: messageText
    });
    return jsonResponse({ ok: false, error: 'queue_enqueue_failed' }, 503);
  }

  return jsonResponse({ ok: true, duplicate: false, eventId: event.eventId }, 202);
}

async function handleQueue(batch: MessageBatch<QueueMessageBody>, env: Env): Promise<void> {
  for (const message of batch.messages) {
    const data = message.body;
    console.info(
      JSON.stringify({
        stage: 'queue_received',
        eventId: data.eventId,
        eventType: data.eventType,
        dedupeKey: data.dedupeKey
      })
    );

    try {
      const result = await sendToHubSpot(data, env);

      if (!result.ok) {
        console.warn(
          JSON.stringify({
            stage: 'hubspot_delivery_failed',
            eventId: data.eventId,
            eventType: data.eventType,
            responseCode: result.responseCode,
            responseBody: result.responseBody
          })
        );
        await updateDeliveryStatus(env.DB, data.eventId, 'failed', {
          responseCode: result.responseCode,
          lastError: result.responseBody
        });
        if (shouldRetryHubSpotStatus(result.responseCode)) {
          message.retry();
          continue;
        }

        message.ack();
        continue;
      }

      console.info(
        JSON.stringify({
          stage: 'hubspot_delivery_succeeded',
          eventId: data.eventId,
          eventType: data.eventType,
          responseCode: result.responseCode,
          responseBody: result.responseBody
        })
      );
      await updateDeliveryStatus(env.DB, data.eventId, 'delivered', {
        responseCode: result.responseCode
      });
      message.ack();
    } catch (error) {
      const messageText = error instanceof Error ? error.message : 'unknown error';
      console.error(
        JSON.stringify({
          stage: 'queue_handler_exception',
          eventId: data.eventId,
          eventType: data.eventType,
          error: messageText
        })
      );
      await updateDeliveryStatus(env.DB, data.eventId, 'failed', {
        lastError: messageText
      });
      message.retry();
    }
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);

    if (request.method === 'GET' && url.pathname === '/health') {
      return jsonResponse({ ok: true, service: 'affirm-hubspot-worker' }, 200);
    }

    if (request.method === 'POST' && url.pathname === '/affirm/webhook') {
      return handleAffirmWebhook(request, env);
    }

    return jsonResponse({ ok: false, error: 'not_found' }, 404);
  },

  async queue(batch: MessageBatch<QueueMessageBody>, env: Env): Promise<void> {
    await handleQueue(batch, env);
  }
} satisfies ExportedHandler<Env, QueueMessageBody>;
