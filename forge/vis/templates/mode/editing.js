window.addEventListener("message", (event) => {
    if (event.source !== directives || !ROOT_URL.startsWith(event.origin) ||
            !event.source.location.href.startsWith(ROOT_URL)) {
        return;
    }
    const data = event.data;
    if (data.type === "EditDirectivesChanged") {
        DataSocket.reloadData();
    }
});

$('#show_pass_data').click(function(event) {
    event.preventDefault();
    showModal('{{ request.url_for("pass_modal", station=station, mode_name=mode.mode_name) }}');
});
