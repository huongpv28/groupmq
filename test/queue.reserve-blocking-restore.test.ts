import Redis from 'ioredis';
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { Queue } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';

/**
 * When reserveBlocking pops a group from :ready (BZPOPMIN) but the atomic
 * reserve returns no job, the group must be put back with the score of its
 * CURRENT head job, not with the (possibly stale) score that was popped.
 */
describe('reserveBlocking restore after a failed reserve', () => {
  const prefix = `test:reserve-blocking-restore:${Date.now()}`;
  let redis: Redis;
  let queues: Queue[] = [];
  let counter = 0;

  const makeQueue = () => {
    const q = new Queue({
      redis: new Redis(REDIS_URL),
      namespace: `${prefix}:${++counter}`,
      scoreFromOrderMs: true,
    });
    queues.push(q);
    return q;
  };

  const readyScore = async (q: Queue, groupId: string) => {
    const s = await redis.zscore(`${q.namespace}:ready`, groupId);
    return s === null ? null : Number(s);
  };

  beforeAll(() => {
    redis = new Redis(REDIS_URL);
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    await Promise.all(queues.map((q) => q.close()));
    queues = [];
  });

  afterAll(async () => {
    const keys = await redis.keys(`groupmq:${prefix}*`);
    if (keys.length) await redis.del(keys);
    await redis.quit();
  });

  it('restores the group with the current head score, not the popped one', async () => {
    const q = makeQueue();
    await q.add({ groupId: 'g1', data: {}, jobId: '200', orderMs: 200 });
    // Stale :ready entry that BZPOPMIN will pop (the group head changed since)
    await redis.zadd(`${q.namespace}:ready`, 999, 'g1');

    vi.spyOn(q, 'reserve').mockResolvedValue(null);
    vi.spyOn(q, 'reserveAtomic').mockResolvedValue(null);

    const job = await q.reserveBlocking(1);
    expect(job).toBeNull();
    expect(await readyScore(q, 'g1')).toBe(200);
  });

  it('does not restore an empty group', async () => {
    const q = makeQueue();
    await redis.zadd(`${q.namespace}:ready`, 5, 'empty');

    vi.spyOn(q, 'reserve').mockResolvedValue(null);
    vi.spyOn(q, 'reserveAtomic').mockResolvedValue(null);

    const job = await q.reserveBlocking(1);
    expect(job).toBeNull();
    expect(await readyScore(q, 'empty')).toBeNull();
  });
});
