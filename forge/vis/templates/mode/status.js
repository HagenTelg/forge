$('#show_passed_summary').click(function(event) {
    event.preventDefault();
    showModal('{{ request.url_for("passed_modal", station=station, mode_name=mode.mode_name) }}');
});