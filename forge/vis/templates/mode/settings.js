const enablePlotScrolling = document.getElementById('enable_plot_scrolling');
if (localStorage.getItem('forge-settings-plot-scroll')) {
    enablePlotScrolling.checked = true;
}

const useDOY = document.getElementById('use_doy');
if (localStorage.getItem('forge-settings-time-format') === 'doy') {
    useDOY.checked = true;
}

$('#apply_settings').click(function(event) {
    event.preventDefault();

    if (enablePlotScrolling.checked) {
        localStorage.setItem('forge-settings-plot-scroll', '1');
    } else {
        localStorage.removeItem('forge-settings-plot-scroll');
    }
    if (useDOY.checked) {
        localStorage.setItem('forge-settings-time-format', 'doy');
    } else {
        localStorage.removeItem('forge-settings-time-format');
    }

    this.blur();
});
