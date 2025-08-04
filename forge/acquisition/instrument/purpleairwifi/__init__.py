
def main():
    from .instrument import Instrument
    from ..http import launch

    launch(Instrument)
