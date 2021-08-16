$('#show_export_data').click(function(event) {
    event.preventDefault();
    showModal('{{ request.url_for("export_modal", station=station, mode_name=mode.mode_name) }}');
});