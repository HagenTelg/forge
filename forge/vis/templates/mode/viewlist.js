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
    const selectView = localStorage.getItem('forge-last-view');
    if (selectView !== null) {
        $("a.view-select[name='" + selectView +"']").click();
    }
    if ($('a.view-select.active').length === 0) {
        $('a.view-select').first().click();
    }
});