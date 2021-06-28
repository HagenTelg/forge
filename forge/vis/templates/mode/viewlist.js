localStorage.setItem('forge-last-station', '{{ station }}');
localStorage.setItem('forge-last-mode', '{{ mode.mode_name }}');

$('a.view-select').click(function(event) {
    event.preventDefault();
    $('a.view-select').not(this).removeClass('active');
    $(this).addClass('active');
    localStorage.setItem('forge-last-view', $(this).attr('name'));
    $('#view_content').load($(this).attr('interior'));
});

$(document).ready(function(event) {
    const queryParameters = new URLSearchParams(window.location.search);
    let selectView = queryParameters.get('view');
    if (selectView === null) {
        selectView = localStorage.getItem('forge-last-view');
    }

    if (selectView !== null) {
        $("a.view-select[name='" + selectView +"']").click();
    }
    if ($('a.view-select.active').length === 0) {
        $('a.view-select').first().click();
    }
});

function isModalActive() {
    const modal = document.getElementById('modal-container');
    return modal.style.display === 'block';
}

function cycleSelected(offset) {
    const active = $('a.view-select.active').first().get(0);
    const selectors = $('a.view-select').get();
    for (let i=0; i<selectors.length; i++) {
        if (selectors[i] !== active) {
            continue;
        }

        i = i + offset;
        if (i < 0) {
            i = selectors.length - 1;
        } else if (i >= selectors.length) {
            i = 0;
        }

        $(selectors[i]).click();
        return;
    }

    if (offset < 0) {
        $('a.view-select').last().click();
    } else {
        $('a.view-select').first().click();
    }
}

$(document).keydown(function(event) {
    switch(event.code) {
    case 'KeyN':
        if (isModalActive()) {
            break;
        }
        event.preventDefault();

        cycleSelected(1);
        break;

    case 'KeyP':
    case 'KeyB':
        if (isModalActive()) {
            break;
        }
        event.preventDefault();

        cycleSelected(-1);
        break;

    case 'KeyR':
        if (isModalActive()) {
            break;
        }
        event.preventDefault();

        if (TimeSelect.zoom_start_ms && TimeSelect.zoom_end_ms) {
            TimeSelect.change(TimeSelect.zoom_start_ms, TimeSelect.zoom_end_ms);
        } else {
            DataSocket.reloadData();
        }
        break;

    case 'KeyA':
        if (isModalActive()) {
            break;
        }
        event.preventDefault();

        TimeSelect.resetTimeRange();
        break;
    }
});