import ray
from tqdm.auto import tqdm

def ray_map(func, els, **kwargs):
    res = []
    for el in els:
        res.append(func.remote(el, **kwargs))
    return res

def ray_get_erred_succeeded(res):
    errors = []
    oks = []
    for i, el in enumerate(res):
        try:
            el_r = ray.get(el)
            oks.append(el_r)
        except Exception as e:
            errors.append((i, el, e))
    return oks, errors

def ray_tasks_progress(res):
    with tqdm(total=len(res)) as pb:
        done = 0
        while True:
            ready, not_ready = ray.wait(res, num_returns=len(res), timeout=5, fetch_local=False)
            ready = len(ready)
            not_ready = len(not_ready)
            if done == ready:
                continue
            else:
                pb.update(ready-done)
                done = ready
            if done == len(res):
                break