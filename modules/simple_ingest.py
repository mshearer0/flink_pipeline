import mgp

@mgp.read_proc
def hello() -> mgp.Record(greeting=str):
    return mgp.Record(greeting="Hello from Python!")
