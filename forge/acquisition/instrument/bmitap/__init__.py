
def main():
    from .instrument import Instrument
    from ..streaming import launch

    launch(Instrument)