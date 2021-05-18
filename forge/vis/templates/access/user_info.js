const email = document.getElementById('email');
const name = document.getElementById('name');
const initials = document.getElementById('initials');

$('.toggle-password').click(function() {
    $(this).toggleClass('mdi-eye mdi-eye-off');
    const input = $($(this).attr('toggle'));
    if (input.attr('type') === 'password') {
        input.attr('type', 'text');
    } else {
        input.attr('type', 'password');
    }
});

$('#apply_changes').click(function(event) {
    event.preventDefault();

    let data = {};
    if (email.getAttribute('original') !== email.value) {
        data.email = email.value;
    }
    if (name.getAttribute('original') !== name.value) {
        data.name = name.value;
    }
    if (initials.getAttribute('original') !== initials.value) {
        data.initials = initials.value;
    }

    this.textContent = "Saving...";

    function clearStatus() {
        $('#apply_changes').text("Save");
        $('#apply_changes').removeClass('error success');
    }
    function success() {
        $('#apply_changes').text("Saved");
        $('#apply_changes').addClass('success')
        setTimeout(clearStatus, 2000);
    }
    function failure() {
        $('#apply_changes').text("ERROR");
        $('#apply_changes').addClass('error')
        setTimeout(clearStatus, 5000);
    }

    $.post("{{ request.url_for('change_user_info') }}", JSON.stringify(data), function(response) {
        if (response.status !== 'ok') {
            failure();
            return;
        }
        success();
        if (response.email !== undefined) {
            email.value = response.email;
            email.setAttribute('original', email.value);
        } else {
            email.value = email.getAttribute('original');
        }
        if (response.name !== undefined) {
            name.value = response.name;
            name.setAttribute('original', name.value);
        } else {
            name.value = name.getAttribute('original');
        }
        if (response.initials !== undefined) {
            initials.value = response.initials;
            initials.setAttribute('original', initials.value);
        } else {
            initials.value = initials.getAttribute('original');
        }
    }).fail(failure);

    this.blur();
});

function passwordVerify() {
    function failed(message) {
        document.getElementById('change_password').setAttribute('disabled', 'disabled');
        document.getElementById('password_error').innerHTML = message;
    }

    const password = document.getElementById('set_password');
    if (password.value.length > 0 && password.value.length < 8) {
        failed("Password must be at least eight characters long");
        return;
    }

    const confirmPassword = document.getElementById('confirm_password');
    if (confirmPassword.value.length > 0 && confirmPassword.value !== password.value) {
        failed("Passwords do not match");
        return;
    }

    if (password.value.length === 0) {
        failed("");
        return;
    }

    document.getElementById('change_password').removeAttribute('disabled');
    document.getElementById('password_error').innerHTML = '';
}
$('.password-field').change(passwordVerify);
$('.password-field').on('input', passwordVerify);
$('#change_password').click(function(event) {
    event.preventDefault();

    let data = {
        'password': document.getElementById('set_password').value,
    };

    this.textContent = "Saving...";

    function clearStatus() {
        $('#change_password').text("Change Password");
        $('#change_password').removeClass('error success');
    }
    function success() {
        $('#change_password').text("Saved");
        $('#change_password').addClass('success')
        setTimeout(clearStatus, 2000);
    }
    function failure() {
        $('#change_password').text("ERROR");
        $('#change_password').addClass('error')
        setTimeout(clearStatus, 5000);
    }

    $.post("{{ request.url_for('change_password') }}", JSON.stringify(data), function(response) {
        if (response.status !== 'ok') {
            failure();
            return;
        }
        success();
    }).fail(failure);

    this.blur();
});