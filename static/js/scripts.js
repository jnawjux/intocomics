// Empty JS for your own code to be here
$(document).ready(function() {
    $("#recommend_button").click(function() {
      // disable button
      $(this).prop("disabled", true);
      // add spinner to button
      $(this).html(
        `<span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>Magic happening...`
      );
    });
});