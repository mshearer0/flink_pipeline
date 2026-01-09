import mgp

@mgp.read_proc
def check() -> mgp.Record(status=str):
    return mgp.Record(status="Internal Bridge Working")
