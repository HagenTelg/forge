const BUTTON_SELECTOR = '#{{uid}} nav.display-tabs button';

function updateVisibility() {
    $(BUTTON_SELECTOR).each(function() {
        $($(this).attr('display')).css('display', 'none');
    });
    $(BUTTON_SELECTOR).filter('.active').each(function() {
        $($(this).attr('display')).css('display', '');
    });
}

$(BUTTON_SELECTOR).click(function(event) {
    event.preventDefault();

    $(this).addClass('active');
    $(BUTTON_SELECTOR).not(this).removeClass('active');
    updateVisibility();
})

$(BUTTON_SELECTOR).first().addClass('active');
updateVisibility();
