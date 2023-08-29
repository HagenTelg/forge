const enablePlotScrolling = document.getElementById('enable_plot_scrolling');
if (localStorage.getItem('forge-settings-plot-scroll')) {
    enablePlotScrolling.checked = true;
}

const enablePlotAutosaveZoom = document.getElementById('enable_plot_autosave_zoom');
if (localStorage.getItem('forge-settings-plot-autosave-zoom')) {
    enablePlotAutosaveZoom.checked = true;
}

const useDOY = document.getElementById('use_doy');
if (localStorage.getItem('forge-settings-time-format') === 'doy') {
    useDOY.checked = true;
}

const alwaysShowEditDirectives = document.getElementById('always_show_edits');
if (localStorage.getItem('forge-always-show-edit-directives')) {
    alwaysShowEditDirectives.checked = true;
}

const viewSwitchRetainZoom = document.getElementById('view_switch_retain_zoom');
if (localStorage.getItem('forge-view-switch-retain-zoom')) {
    viewSwitchRetainZoom.checked = true;
}

$('#apply_settings').click(function(event) {
    event.preventDefault();

    if (enablePlotScrolling.checked) {
        localStorage.setItem('forge-settings-plot-scroll', '1');
    } else {
        localStorage.removeItem('forge-settings-plot-scroll');
    }
    if (enablePlotAutosaveZoom.checked) {
        localStorage.setItem('forge-settings-plot-autosave-zoom', '1');
    } else {
        localStorage.removeItem('forge-settings-plot-autosave-zoom');
    }
    if (useDOY.checked) {
        localStorage.setItem('forge-settings-time-format', 'doy');
    } else {
        localStorage.removeItem('forge-settings-time-format');
    }
    if (alwaysShowEditDirectives.checked) {
        localStorage.setItem('forge-always-show-edit-directives', '1');
    } else {
        localStorage.removeItem('forge-always-show-edit-directives');
    }
    if (viewSwitchRetainZoom.checked) {
        localStorage.setItem('forge-view-switch-retain-zoom', '1');
    } else {
        localStorage.removeItem('forge-view-switch-retain-zoom');
    }

    this.blur();
});
