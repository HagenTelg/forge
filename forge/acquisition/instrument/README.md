[TOC]

# Instrument Quick Reference

## Acoem (formerly Ecotech) NE-x00 Nephelometer

```toml
[instrument.S11]
type = "acoemnex00nephelometer"
serial_port = "/dev/serial/by-id/..."
```

- Instrument baud rate: 38400
- Instrument protocol setting: Acoem
- Display letter: N
- Zero and Spancheck acquisition control not supported

## Aerosol Dynamics MAGIC 200 CPC

```toml
[instrument.N71]
type = "admagic200cpc"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

- Instrument baud rate: 115200
- Display letter: C

## Aerosol Dynamics MAGIC 250 CPC

```toml
[instrument.N71]
type = "admagic250cpc"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

- Instrument baud rate: 115200
- Display letter: C

## BMI MCPC 1710

```toml
[instrument.N71]
type = "bmi1710cpc"
serial_port = "/dev/serial/by-id/..."
cut_size = false
serial_number = 123
```

- Instrument baud rate: 38400
- Display letter: C

## BMI MCPC 1720

```toml
[instrument.N71]
type = "bmi1720cpc"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

- Instrument baud rate: 38400
- Display letter: C

## BMI TAP

```toml
[instrument.A11]
type = "bmitap"
serial_port = "/dev/serial/by-id/..."
hardware_flow_calibration = [-0.65311, 0.69929, -0.19473, 0.04253] # 10.028#2020-01-29/PJS@GMD
[instrument.A11.data.Q]
scale = 0.995
```

- Instrument baud rate: 57600
- Display letter: W
- Spot size: 20.1

Use the spot recommended by BMI based on the filter type:

```toml
[instrument.A11]
# ...
spot = 25.28
```

## GML CLAP

```toml
[instrument.A11]
type = "clap"
serial_port = "/dev/serial/by-id/..."
spot = [
    19.67,
    20.17,
    20.23,
    20.50,
    19.88,
    20.05,
    19.80,
    19.50,
]
hardware_flow_calibration = [-0.65311, 0.69929, -0.19473, 0.04253] # 10.028#2020-01-29/PJS@GMD
[instrument.A11.data.Q]
scale = 0.995
```

- Instrument baud rate: 57600
- Display letter: W
- Spot size: 19.9

## CSD/CSL POPS

```toml
[instrument.N11]
type = "csdpops"
serial_port = "/dev/serial/by-id/..."
cut_size = false
diameter = [
    0.130, 0.131, 0.132, # ...
]
[instrument.N11.data]
Q = [0.0, 1.082]
```

- Instrument baud rate: 115200
- Display letter: P

## DMT CCN

```toml
[instrument.N11]
type = "dmtccn"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

- Instrument baud rate: 9600
- Display letter: N

## Ecotech Aurora 3000/4000 Nephelometer

```toml
[instrument.S11]
type = "ecotechnephelometer"
serial_port = "/dev/serial/by-id/..."
```

- Instrument baud rate: 38400
- Display letter: N
- Kalman filter disabled (automatically, if the firmware supports it)
- Zero mode: Offset (software subtraction based on measurement)
- Zero scheduled at: [PT54M59S](https://en.wikipedia.org/wiki/ISO_8601) (54:59 after the hour)
- 62 seconds to flush on sample air
- 180 seconds to fill with zero air
- 300 seconds to measure zero air
- Spancheck CO<sub>2</sub> sample: 600 seconds, flush: 480 seconds
- Spancheck air sample: 600 seconds, flush: 480 seconds

## Gill Instruments Windsonic Anemometer

```toml
[instrument.XM1]
type = "gillwindsonic"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

- Instrument baud rate: 9600
- Display letter: I

## Grimm 1.10x OPC

```toml
[instrument.N71]
type = "grimm110xopc"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```
- Instrument baud rate: 9600
- Display letter: N

## Magee AE31 Aethalometer

```toml
[instrument.A81]
type = "mageeae31"
serial_port = "/dev/serial/by-id/..."
average = 300
cut_size = false
```

- Instrument baud rate: 9600
- Display letter: E
- Report interval (also match above averaging): 300 seconds (5 minutes)
- Spot size: 50 mm<sup>2</sup>

## Magee AE33 Aethalometer

```toml
[instrument.A81]
type = "mageeae33"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

- Instrument baud rate: 115200
- Display letter: E
- Instrument time base set on the front panel

## Magee AE36 Aethalometer

Direction serial communication using ADP:
```toml
[instrument.A81]
type = "mageeae36"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

ADP over network:
```toml
[instrument.A81]
type = "mageeae36"
tcp = "140.172.50.74:8003"
cut_size = false
```

UIDEP over HTTP:
```toml
[instrument.A81]
type = "mageeae36"
url = "http://140.172.50.74"
cut_size = false
```

- Instrument baud rate: 115200
- Display letter: E
- Instrument time base set on the front panel

## Purple Air

Direct USB connection:
```toml
[instrument.A81]
type = "purpleairusb"
serial_port = "/dev/serial/by-id/..."
average = 120
cut_size = false
```

Wi-Fi networked connection:
```toml
[instrument.A81]
type = "purpleairwifi"
url = "http://192.168.0.98"
```

- Instrument baud rate: 115200
- Display letter: A
- Default URL: `/json?live=true`

## RMY 86xxx Series Sonic Anemometer

```toml
[instrument.XM1]
type = "rmy86xxx"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

- Instrument baud rate: 9600
- Display letter: I

## 2B Tech 205 Ozone Monitor

```toml
[instrument.G81]
type = "tech2b205"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

- Instrument baud rate: 2400 
- Display letter: Z
- Configured baud rate displayed on instrument LCD after power on

## Teledyne N500

```toml
[instrument.G82]
type = "teledynen500"
tcp = "140.172.50.74:502"
cut_size = false
serial_number = 1533
```

- The IP or host name must match the instrument
- Port "2" on instrument front panel must be enabled and match the above (the ":2" above).  Any valid TCP port (1-65535) can be used as long as they match.
- Display letter: G

## Teledyne T640

```toml
[instrument.M11]
type = "teledynet640"
tcp = "140.172.50.75:2"
cut_size = false
serial_number = 1533
```

- The IP or host name must match the instrument
- Port "2" on instrument front panel must be enabled and match the above (the ":2" above).  Any valid TCP port (1-65535) can be used as long as they match.
- Display letter: M

## Thermo 49i or 49c Ozone Monitor

```toml
[instrument.G81]
type = "thermo49"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

- Instrument baud rate: 9600
- Display letter: Z

## Thermo 49iQ Ozone Monitor

```toml
[instrument.G81]
type = "thermo49iq"
tcp = "140.172.50.74:502"
cut_size = false
```

- The IP or host name must match the instrument
- The port (502 above) must match the MODBUS port set on the instrument
- Display letter: Z

## Thermo MAAP

```toml
[instrument.A21]
type = "thermomaap"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

- Instrument baud rate: 9600 (7 data bits, even parity, 2 stop bits)
- Display letter: A
- PF12 requires COM1 (DB-25) and a NULL modem

## TSI 3563 Nephelometer

```toml
[instrument.S11]
type = "tsi3563nephelometer"
serial_port = "/dev/serial/by-id/..."
serial_number = 1077
```

- Instrument baud rate: 9600 (7 data bits, even parity)
- Display letter: N
- Zero scheduled at: [PT56M58S](https://en.wikipedia.org/wiki/ISO_8601) (56:59 after the hour)
- Spancheck CO<sub>2</sub> sample: 300 seconds, flush: 600 seconds
- Spancheck air sample: 600 seconds, flush: 180 seconds

## TSI 375x and 3789 CPC

RS-232 serial connection:
```toml
[instrument.N71]
type = "tsi375xcpc"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

Network or USB connection:
```toml
[instrument.N71]
type = "tsi375xcpcnet"
tcp = "169.254.15.133:3603"
cut_size = false
```

- Instrument baud rate: 115200
- Display letter: C
- Flow rate depends on instrument type, calibration is for the sample/nozzle flow only

## TSI 377x CPC

```toml
[instrument.N71]
type = "tsi377xcpc"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

- Instrument baud rate: 115200
- Display letter: C
- Flow rate depends on instrument type, calibration is for the sample/nozzle flow only

## TSI 302x CPC

```toml
[instrument.N71]
type = "tsi302xcpc"
serial_port = "/dev/serial/by-id/..."
cut_size = false
serial_number = 2217
```

- Instrument baud rate: 9600 (7 data bits, even parity)
- Display letter: C
- Default flow: 0.3 lpm (3022) or 0.03 lpm (3025), calibration is for the sample flow only

## TSI 3010 CPC

```toml
[instrument.N71]
type = "tsi3010cpc"
serial_port = "/dev/serial/by-id/..."
cut_size = false
serial_number = 2217
[instrument.N71.data]
Q = 0.979 # 200128/JPS@APP
```

- Instrument baud rate: 9600 (7 data bits, even parity)
- Display letter: C
- Default flow: 1.0 lpm

## TSI 3760 CPC

```toml
[instrument.N71]
type = "tsi3760cpc"
serial_port = "/dev/serial/by-id/..."
cut_size = false
serial_number = 388
[instrument.N71.data]
Q = 1.536 # 2022-12-09/PJS@BOS
```

- Instrument baud rate: 9600
- Display letter: C
- Default flow: 1.4210 lpm

## TSI 3781 CPC

```toml
[instrument.N71]
type = "tsi3781cpc"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

- Instrument baud rate: 115200
- Display letter: C
- Default flow: 0.12 lpm

## TSI 3783 CPC

```toml
[instrument.N71]
type = "tsi3783cpc"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

- Instrument baud rate: 115200
- Display letter: C
- Default flow: 0.12 lpm

## TSI 4000 and 5000 series Mass Flow Meter

```toml
[instrument.Q61]
type = "tsimfm"
serial_port = "/dev/serial/by-id/..."
cut_size = false
display_letter = "F"
[instrument.Q61.data]
Q = [ -0.046, 1.6624 ] # 2015-05-05 PJS@MLO
```

- Instrument baud rate: 38400
- Display letter: Q

## Vaisala PWDx2 series weather monitor

```toml
[instrument.XM1]
type = "vaisalapwdx2"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

- Instrument baud rate: 9600 (7 data bits, even parity)
- Display letter: V

## Vaisala WMT 700 Sonic Anemometer

```toml
[instrument.XM1]
type = "vaisalawmt700"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

- Instrument baud rate: 9600
- Display letter: I

## Vaisala WXT 5xx series Weather Monitor

```toml
[instrument.XM1]
type = "vaisalawxt5xx"
serial_port = "/dev/serial/by-id/..."
cut_size = false
```

- Instrument baud rate: 19200
- Display letter: I

## Azonix 𝜇MAC 1050

```toml
[instrument.X1]
type = "azonixumac1050"
serial_port = "/dev/serial/by-id/..."
[instrument.X1.digital.EnableAnalyzerFlow]
channel = 0
bypass = false
shutdown = false
initial = true
[instrument.X1.digital.PM1Impactor]
channel = 1
cut_size = "pm1"

[instrument.X1.data.Pd_P01]
channel = 19
description = "stack pitot tube"
calibration = [-0.5038, 4.5234] # 20190730eja
cut_size = false

[instrument.X1.data.Pd_P11]
channel = 20
description = "impactor pressure drop"
calibration = [-202.16, 327.650] # 20221011/PJS@BOS

[instrument.X1.data.T_V11]
channel = 9
description = "impactor box inlet temperature"
calibration = [-40, 100]

#...
```

- Instrument baud rate: 9600
- Display letter: U

## Campbell CR1000 with GML control program

```toml
[instrument.X1]
type = "campbellcr1000gmd"
serial_port = "/dev/serial/by-id/..."
[instrument.X1.digital.EnableAnalyzerFlow]
channel = 0
bypass = false
shutdown = false
initial = true
[instrument.X1.digital.PM1Impactor]
channel = 1
cut_size = "pm1"

[instrument.X1.data.Pd_P01]
channel = 19
description = "stack pitot tube"
calibration = [-0.5038, 4.5234] # 20190730eja
cut_size = false

[instrument.X1.data.Pd_P11]
channel = 20
description = "impactor pressure drop"
calibration = [-202.16, 327.650] # 20221011/PJS@BOS

[instrument.X1.data.T_V11]
channel = 9
description = "impactor box inlet temperature"
calibration = [-40, 100]

#...
```

- Instrument baud rate: 115200
- Display letter: U

## Love PID Controller Box

```toml
[instrument.X2]
type = "lovepid"
serial_port = "/dev/serial/by-id/..."

[instrument.X2.data.Q_Q11]
address = 0x34
type = "sample_flow"
description = "analyzer flow"
calibration = [1.0425, 0.646600] # 20221012/PJS@BOS
[instrument.X2.data.Q_Q11.output]
remember_changes = false
initial = 32.3

[instrument.X2.data.U_V11]
address = 0x32
description = "impactor box inlet RH"
[instrument.X2.data.U_V11.output]
remember_changes = false
initial = 40.0
```

- Instrument baud rate: 9600
- Display letter: P
- RS-485 control interface
- Controller 0x34 (the one in the MFC spot) defaults to manual mode output

Remove the "output" subsection to prevent make the acquisition system automatically restore the last value on startup.

## Books 0254 PID controller

```toml
[instrument.X1]
type = "brooks0254"
serial_port = "/dev/serial/by-id/..."

[instrument.X1.data.Q_Q11]
channel = 1
type = "sample_flow"
description = "analyzer flow"
calibration = [1.0425, 0.646600] # 20221012/PJS@BOS
[instrument.X1.data.Q_Q11.output]
remember_changes = false
initial = 32.3
```

- Instrument baud rate: 9600
- Display letter: P

# Common Configuration Tasks

## Add a secondary instrument

```toml
[instrument.N72]
# ...
display_letter = "D"
tags = "secondary"
```

Noter: the `display_letter = "D"` is optional; it just overrides the default display letter so there is no duplication.

## Flow Calibration

Constant:
```toml
[instrument.N71]
# ...
[instrument.N71.data]
Q = 1.536 # 2022-12-09/PJS@BOS
```

Polynomial:
```toml
[instrument.N71]
# ...
[instrument.N71.data]
Q = [0.123, 1.1] # 2022-12-09/PJS@BOS
```

## Setting CLAP/TAP advance transmittance

```toml
[instrument.A11]
# ...
advance_transmittance = [0.3, 0.5, 0.3]
```

The channel order is blue, green, red.

## Override the baud rate

Replace `serial_port = ...` with:

```toml
[instrument.S11.serial_port]
port = "/dev/serial/by-id/..."
baud = 38400
```

## Set nephelometer zero schedule

Specified in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601).  May also be specified as "MM:SS".

```toml
[instrument.S11]
# ...
zero = "PT54M59S"
```

Disabled:

```toml
[instrument.S11]
# ...
zero = false
```

## Set TSI 3563 nephelometer parameters

```toml
[instrument.S11]
# ...
[instrument.S11.parameters]
B = 0
SMZ = 4
```

| Parameter | Default | Description                                         |
|-----------|---------|-----------------------------------------------------|
| SMZ       | 1       | Autozero mode (0 = off, 1 = single, 2-24 = average) |
| STA       | 1       | Averaging time                                      |
| STB       | 62      | Zero blank time                                     |
| STP       | 32000   | Autozero interval                                   | 
| STZ       | 300     | Zero measurement time                               |
| SMB       | 1       | Enable backscatter                                  |
| SP        | 75      | Lamp power (watts)                                  |
| B         | 255     | Blower power (0-255)                                |
| SKB       |         | Blue calibration                                    |
| SKG       |         | Green calibration                                   |
| SKR       |         | Red calibration                                     |
| SVB       |         | Blue PMT voltage                                    |
| SVG       |         | Green PMT voltage                                   |
| SVR       |         | Red PMT voltage                                     |
| H         |         | Enable heater                                       |
| SL        |         | Calibration label                                   |

## Second channel on 3760 pulse counter box

This assumes a N71 counter on channel 1, for channel 2 remove the `serial_port = ...` line.

```toml
[instrument.N72]
type = "tsi3760cpc"
unix_socket = "/run/forge-serial-N71/raw.sock"
channel = 2
# ...
```

## Changing instrument report interval

```toml
[instrument.A81]
# ...
report_interval = 120
```

# Cut Size Control

For systems with whole air inlets (e.x. surface ozone only), simply omit any definition.  The `cut_size = false` referenced above can be removed in this case.

## Standard Alternating Sizes

This has PM1 at the top of the hour, changing every 6 minutes.

```toml
[acquisition.cut_size]
schedule = "PM1"
alternate = { size = "PM10", interval = "6M" }
```

## Fixed Inlet Size

```toml
[acquisition]
cut_size = "PM2.5"
```

