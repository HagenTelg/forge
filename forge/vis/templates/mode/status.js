$('#show_passed_summary').click(function(event) {
    event.preventDefault();
    showModal('{{ request.url_for("passed_modal", station=station, mode_name=mode.mode_name) }}');
});

$('#show_instrument_summary').click(function(event) {
    event.preventDefault();
    showModal('{{ request.url_for("instruments_modal", station=station, mode_name=mode.mode_name) }}');
});