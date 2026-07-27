# v11.2 — the app boots in the WORKER, never the master. preload is explicitly
# off, and even if some environment forces it, post_worker_init below still
# runs inside the worker, so the scheduler, structure, and state always live
# in the same process that serves HTTP. This ends the split-brain class of
# bugs (master trading invisibly while a forked worker answers /status blank).
preload_app = False
workers = 1
threads = 8
timeout = 120

def post_worker_init(worker):
    import main
    main._boot_in_worker()
