import typing
from ..default.flags import CPD3Flag


lookup: typing.Dict[str, CPD3Flag] = {
    'spot_advancing': CPD3Flag("SpotAdvanced", "The spot has just advanced and data may be suspect"),
    'not_measuring': CPD3Flag("NotMeasuring", "Not measuring due to tap advance or fast calibration", 0x00010000),
    'calibrating': CPD3Flag("Calibrating", "Calibration of LED, flow, and/or tape sensors in progress", 0x00020000),
    'stopped': CPD3Flag("Stopped", "Instrument in stop mode", 0x00030000),
    'flow_out_of_range': CPD3Flag("FlowOutOfRange", "Flow above or below target by more than 0.25 lpm", 0x00040000),
    'flow_check_history': CPD3Flag("FlowCheckHistory", "Flow check status history", 0x00080000),
    'led_calibration': CPD3Flag("LEDCalibration", "LED calibration in progress", 0x00100000),
    'led_calibration_error': CPD3Flag("LEDCalibrationError", "LED calibration error detected", 0x00200000),
    'led_error': CPD3Flag("LEDCalibrationError", "All channels failed calibration or no communications with the LED module", 0x00300000),
    'chamber_error': CPD3Flag("ChamberError", "Chamber error detected", 0x00400000),
    'tape_low': CPD3Flag("ChamberError", "Less than 10 spots remaining", 0x01000000),
    'tape_critical': CPD3Flag("TapeCritical", "Less than five spots remaining", 0x02000000),
    'tape_error': CPD3Flag("TapeError", "Out of tape or tape not moving", 0x03000000),
    'stability_test': CPD3Flag("StabilityTest", "Stability test in progress", 0x04000000),
    'clear_air_test': CPD3Flag("CleanAirTest", "Clean air test in progress", 0x08000000),
    'change_tape_test': CPD3Flag("ChangeTapeTest", "Tape change test in progress", 0x0C000000),
    'controller_not_ready': CPD3Flag("ControllerNotReady", "Controller reset and not ready (no date set)"),
    'controller_busy': CPD3Flag("ControllerBusy", "Controller busy"),
    'detector_initialization_error': CPD3Flag("DetectorInitializationError", "Detector initialization error reported"),
    'detector_stopped': CPD3Flag("DetectorStopped", "Detector in stop state"),
    'detector_led_calibration': CPD3Flag("DetectorLEDCalibration", "Detector in LED calibration state"),
    'detector_fast_led_calibration': CPD3Flag("DetectorFastLEDCalibration", "Detector in fast LED calibration state"),
    'detector_read_ndf0': CPD3Flag("DetectorReadNDF0", "Detector in read NDF0 state"),
    'detector_read_ndf1': CPD3Flag("DetectorReadNDF1", "Detector in read NDF1 state"),
    'detector_read_ndf2': CPD3Flag("DetectorReadNDF2", "Detector in read NDF2 state"),
    'detector_read_ndf3': CPD3Flag("DetectorReadNDF3", "Detector in read NDF3 state"),
    'detector_read_ndf_error': CPD3Flag("DetectorReadNDFError", "Detector NDF error"),
}
