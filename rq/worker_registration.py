from .compat import as_text


WORKERS_BY_QUEUE_KEY = 'rq:workers:%s'  # [z]: The name for the set the contains worker keys for the queue
REDIS_WORKER_KEYS = 'rq:workers'  # [z]: The name for the set that contains all the worker keys


def register(worker, pipeline=None):
    """Store worker key in Redis so we can easily discover active workers."""
    connection = pipeline if pipeline is not None else worker.connection
    connection.sadd(worker.redis_workers_keys, worker.key)  # [z]: Add worker key to set `rq:workers`
    for name in worker.queue_names():
        redis_key = WORKERS_BY_QUEUE_KEY % name
        connection.sadd(redis_key, worker.key)
    # [z]: For each queue of the worker, add worker key to set `rq:workers:<queue_name>`


def unregister(worker, pipeline=None):
    """Remove worker key from Redis."""
    if pipeline is None:
        connection = worker.connection.pipeline()
    else:
        connection = pipeline

    connection.srem(worker.redis_workers_keys, worker.key)  # [z]: Remove worker key from set `rq:workers`
    for name in worker.queue_names():
        redis_key = WORKERS_BY_QUEUE_KEY % name
        connection.srem(redis_key, worker.key)
    # [z]: For each queue of the worker, remove worker key from set `rq:workers:<queue_name>`

    if pipeline is None:
        connection.execute()


def get_keys(queue=None, connection=None):
    """Returnes a list of worker keys for a queue"""
    if queue is None and connection is None:
        raise ValueError('"queue" or "connection" argument is required')

    if queue:
        redis = queue.connection
        redis_key = WORKERS_BY_QUEUE_KEY % queue.name
        # [z] If a queue is given, return all the keys from set `rq:workers:<queue_name>`
    else:
        redis = connection
        redis_key = REDIS_WORKER_KEYS
        # [z] If queue is not given, return all the keys from set `rq:workers` (all the workers for all queues)

    return {as_text(key) for key in redis.smembers(redis_key)}
    # [z] Return a set of worker keys


def clean_worker_registry(queue):
    """Delete invalid worker keys in registry"""
    keys = list(get_keys(queue))

    with queue.connection.pipeline() as pipeline:

        for key in keys:
            pipeline.exists(key)
        results = pipeline.execute()

        invalid_keys = []

        for i, key_exists in enumerate(results):
            if not key_exists:
                invalid_keys.append(keys[i])
        # [z]: It seems like if a worker key does not exist as a redis key, it is invalid.
        # [z?]: Find out more about the usage of the worker key.

        if invalid_keys:
            pipeline.srem(WORKERS_BY_QUEUE_KEY % queue.name, *invalid_keys)
            # [z]: Remove from set `zq:workers:<queue_name>`
            pipeline.srem(REDIS_WORKER_KEYS, *invalid_keys)
            # [z]: Remove from set `zq:workers`
            pipeline.execute()
