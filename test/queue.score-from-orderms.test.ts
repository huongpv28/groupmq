import Redis from 'ioredis';
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { Queue } from '../src';

const REDIS_URL = process.env.REDIS_URL ?? 'redis://127.0.0.1:6379';
const BASE_EPOCH_MS = 1704067200000;

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe('Queue scoreFromOrderMs option', () => {
  const prefix = `test:score-orderms:${Date.now()}`;
  let redis: Redis;
  let queues: Queue[] = [];
  let counter = 0;

  const makeQueue = (opts: Record<string, unknown> = {}) => {
    const namespace = `${prefix}:${++counter}`;
    const q = new Queue({
      redis: new Redis(REDIS_URL),
      namespace,
      ...opts,
    } as any);
    queues.push(q);
    return q;
  };

  const jobHash = (q: Queue, jobId: string) =>
    redis.hgetall(`${q.namespace}:job:${jobId}`);

  const groupScore = async (q: Queue, groupId: string, jobId: string) => {
    const s = await redis.zscore(`${q.namespace}:g:${groupId}`, jobId);
    return s === null ? null : Number(s);
  };

  const readyScore = async (q: Queue, groupId: string) => {
    const s = await redis.zscore(`${q.namespace}:ready`, groupId);
    return s === null ? null : Number(s);
  };

  beforeAll(() => {
    redis = new Redis(REDIS_URL);
  });

  afterEach(async () => {
    await Promise.all(queues.map((q) => q.close()));
    queues = [];
  });

  afterAll(async () => {
    const keys = await redis.keys(`groupmq:${prefix}*`);
    if (keys.length) await redis.del(keys);
    await redis.quit();
  });

  describe('enabled', () => {
    it('stores score == orderMs, seq "0" and orderMs on the job hash', async () => {
      const q = makeQueue({ scoreFromOrderMs: true });
      await q.add({
        groupId: 'g1',
        data: { n: 1 },
        jobId: '6009048',
        orderMs: 6009048,
      });

      const h = await jobHash(q, '6009048');
      expect(h.score).toBe('6009048');
      expect(h.seq).toBe('0');
      expect(h.orderMs).toBe('6009048');
      expect(await groupScore(q, 'g1', '6009048')).toBe(6009048);
      expect(await readyScore(q, 'g1')).toBe(6009048);
    });

    it('does not increment the sequence counter', async () => {
      const q = makeQueue({ scoreFromOrderMs: true });
      await redis.set(`${q.namespace}:seq:0`, '5652876');
      await q.add({ groupId: 'g1', data: {}, jobId: '1', orderMs: 1 });
      expect(await redis.get(`${q.namespace}:seq:0`)).toBe('5652876');
    });

    it('writes the hash score as an exact integer string for large orderMs', async () => {
      const q = makeQueue({ scoreFromOrderMs: true });
      const orderMs = 1234567890123456; // < 2^53, > 14 significant digits
      await q.add({ groupId: 'g1', data: {}, jobId: 'big', orderMs });

      const h = await jobHash(q, 'big');
      expect(h.score).toBe('1234567890123456');
      expect(await groupScore(q, 'g1', 'big')).toBe(orderMs);
    });

    it('reserves by orderMs regardless of a large, moving sequence counter', async () => {
      const q = makeQueue({ scoreFromOrderMs: true });
      const seqKey = `${q.namespace}:seq:0`;
      await redis.set(seqKey, '5652876');

      await q.add({
        groupId: 'g1',
        data: {},
        jobId: '6009048',
        orderMs: 6009048,
      });
      // Other producers keep incrementing the shared counter between enqueues
      await redis.incrby(seqKey, 100000);
      await q.add({
        groupId: 'g1',
        data: {},
        jobId: '6008994',
        orderMs: 6008994,
      });

      const first = await q.reserve();
      expect(first?.id).toBe('6008994');
      expect(await redis.zrange(`${q.namespace}:g:g1`, 0, -1)).toEqual([
        '6009048',
      ]);
      await q.completeWithMetadata(first!, null, {
        processedOn: Date.now(),
        finishedOn: Date.now(),
        attempts: 0,
        maxAttempts: 3,
      });
      const second = await q.reserve();
      expect(second?.id).toBe('6009048');
    });

    it('keeps score == orderMs after retry with back-off and delayed promotion', async () => {
      const q = makeQueue({ scoreFromOrderMs: true });
      await q.add({ groupId: 'g1', data: {}, jobId: '42', orderMs: 42 });

      const job = await q.reserve();
      expect(job?.id).toBe('42');
      await q.retry('42', 50);

      expect((await jobHash(q, '42')).status).toBe('delayed');
      expect(await groupScore(q, 'g1', '42')).toBe(42);

      await sleep(80);
      await q.promoteDelayedJobsBounded(32, Date.now());

      expect((await jobHash(q, '42')).score).toBe('42');
      expect(await groupScore(q, 'g1', '42')).toBe(42);
      expect(await readyScore(q, 'g1')).toBe(42);
    });

    it('keeps score == orderMs for an explicitly delayed enqueue', async () => {
      const q = makeQueue({ scoreFromOrderMs: true });
      await q.add({
        groupId: 'g1',
        data: {},
        jobId: '77',
        orderMs: 77,
        delay: 50,
      });

      expect((await jobHash(q, '77')).status).toBe('delayed');
      expect(await groupScore(q, 'g1', '77')).toBe(77);

      await sleep(80);
      await q.promoteDelayedJobsBounded(32, Date.now());

      expect(await groupScore(q, 'g1', '77')).toBe(77);
      expect(await readyScore(q, 'g1')).toBe(77);
    });

    it('keeps score == orderMs after stalled recovery', async () => {
      const q = makeQueue({ scoreFromOrderMs: true, jobTimeoutMs: 50 });
      await q.add({ groupId: 'g1', data: {}, jobId: '99', orderMs: 99 });

      const job = await q.reserve();
      expect(job?.id).toBe('99');
      expect(await groupScore(q, 'g1', '99')).toBeNull();

      await sleep(100);
      const results = await q.checkStalledJobs(Date.now(), 0, 3);
      expect(results).toEqual(['99', 'g1', 'recovered']);

      expect(await groupScore(q, 'g1', '99')).toBe(99);
      expect(await readyScore(q, 'g1')).toBe(99);
    });

    it('applies to the autoBatch (enqueue-batch) path', async () => {
      const q = makeQueue({ scoreFromOrderMs: true, autoBatch: true });
      await redis.set(`${q.namespace}:seq:0`, '5652876');
      await Promise.all([
        q.add({ groupId: 'g1', data: {}, jobId: '6009048', orderMs: 6009048 }),
        q.add({ groupId: 'g1', data: {}, jobId: '6008994', orderMs: 6008994 }),
      ]);

      for (const id of ['6009048', '6008994']) {
        const h = await jobHash(q, id);
        expect(h.score).toBe(id);
        expect(h.seq).toBe('0');
        expect(h.orderMs).toBe(id);
        expect(await groupScore(q, 'g1', id)).toBe(Number(id));
      }
      expect(await readyScore(q, 'g1')).toBe(6008994);
    });

    it('applies to repeating job instances', async () => {
      const q = makeQueue({ scoreFromOrderMs: true });
      await q.add({
        groupId: 'rg',
        data: {},
        orderMs: 555,
        repeat: { every: 1000 },
      });

      const processed = await q.processRepeatingJobsBounded(
        16,
        Date.now() + 5000,
      );
      expect(processed).toBe(1);

      const ids = await redis.zrange(`${q.namespace}:g:rg`, 0, -1);
      expect(ids).toHaveLength(1);
      const h = await jobHash(q, ids[0]);
      expect(h.score).toBe('555');
      expect(h.seq).toBe('0');
      expect(await groupScore(q, 'rg', ids[0])).toBe(555);
    });
  });

  describe('disabled (default)', () => {
    it('keeps the previous formula: (orderMs - base) * 1000 + seq, seq > 0', async () => {
      const q = makeQueue();
      const orderMs = Date.now();
      await q.add({ groupId: 'g1', data: {}, jobId: 'a', orderMs });

      const h = await jobHash(q, 'a');
      const seq = Number(h.seq);
      expect(seq).toBeGreaterThan(0);
      const expected = (orderMs - BASE_EPOCH_MS) * 1000 + seq;
      expect(Number(h.score)).toBe(expected);
      expect(await groupScore(q, 'g1', 'a')).toBe(expected);
      expect(
        await redis.get(`${q.namespace}:seq:${Math.floor(orderMs / 86400000)}`),
      ).toBe(String(seq));
    });

    it('keeps the previous formula on the autoBatch path', async () => {
      const q = makeQueue({ autoBatch: true });
      const orderMs = Date.now();
      await q.add({ groupId: 'g1', data: {}, jobId: 'b', orderMs });

      const h = await jobHash(q, 'b');
      const seq = Number(h.seq);
      expect(seq).toBeGreaterThan(0);
      expect(Number(h.score)).toBe((orderMs - BASE_EPOCH_MS) * 1000 + seq);
    });

    it('writes the hash score as an exact integer string when it exceeds 14 digits', async () => {
      const q = makeQueue();
      const orderMs = BASE_EPOCH_MS + 200_000_000_000; // score ~ 2e14
      await q.add({ groupId: 'g1', data: {}, jobId: 'c', orderMs });

      const h = await jobHash(q, 'c');
      const seq = Number(h.seq);
      const expected = (orderMs - BASE_EPOCH_MS) * 1000 + seq;
      expect(h.score).toBe(String(expected));
      expect(await groupScore(q, 'g1', 'c')).toBe(expected);
    });
  });
});
