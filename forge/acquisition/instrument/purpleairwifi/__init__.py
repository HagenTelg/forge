
def main():
    from .instrument import Instrument
    from ..run import launch

    launch(Instrument)
