from forge.acquisition.instrument.clap.simulator import Simulator

if __name__ == '__main__':
    from forge.acquisition.serial.simulator import parse_arguments, run
    run(parse_arguments(), Simulator)
