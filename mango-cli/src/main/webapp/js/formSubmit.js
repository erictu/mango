/*****
* Handles form submission that triggers alignment and variant
* rendering
***/

// Autocomplete function for input submit forms
function autoComplete(dictionary) {
    $('#autocomplete').autocomplete({
      lookup: dictionary,
      onSelect: function (suggestion) {
        var thehtml = '<strong>Name:</strong> '+suggestion.value;
        $('#outputcontent').html(thehtml);
      }
    });
}


// on enter submits search form for chromosomal position
$('.variant-search').keypress(function (e) {
  if (e.which == 13) {
    checkVariantForm(this.form);
    return false;
  }
});

// Get and validate form info, reference regions
function validateFormElements(form) {
  var info = form.info.value;

  // validate that input has correct form
  try {
      var refName = info.split(":")[0];
      var region = info.split(":")[1].split("-");
      var newStart = Math.max(0, region[0]);
      var newEnd = Math.max(newStart, region[1]);
  }
  catch(err) {
      form[0].style.borderColor = "red";
      return undefined;
  }

  // Check that the each form input is correct
  if ( info===""|| refName==""|| isNaN(newStart) || isNaN(newEnd) || newStart>=newEnd){
    form[0].style.borderColor = "red";
    return undefined;
  }
  else{
    form[0].style.borderColor = "";
    setGlobalReferenceRegion(refName, newStart, newEnd);

    return {
            refName: refName,
            newStart: newStart,
            newEnd: newEnd
        };
  }
}

// Redirect based on form input
function checkForm(form) {
  var elements = validateFormElements(form);
  if (elements != undefined) {
    render(elements.refName, elements.newStart, elements.newEnd);
  }
}

function checkVariantForm(form) {
  var elements = validateFormElements(form);
  if (elements != undefined) {
    renderVariants(elements.refName, elements.newStart, elements.newEnd);
  }
}

// Try to move very far left
function moveVeryFarLeft() {
  if (validRegion()) {
    var newStart = Math.max(0, viewRegStart - (viewRegEnd-viewRegStart));
    var newEnd = Math.max(newStart, viewRegEnd - (viewRegEnd-viewRegStart));
    render(viewRefName, newStart, newEnd);
  }
}

// Try to move far left
function moveFarLeft() {
  if (validRegion()) {
    var newStart = Math.max(0, viewRegStart - Math.floor((viewRegEnd-viewRegStart)/2));
    var newEnd = Math.max(newStart, viewRegEnd - Math.floor((viewRegEnd-viewRegStart)/2));
    render(viewRefName, newStart, newEnd);
  }
}

// Try to move left
function moveLeft() {
  if (validRegion()) {
    var newStart = Math.max(0, viewRegStart - Math.floor((viewRegEnd-viewRegStart)/4));
    var newEnd = Math.max(newStart, viewRegEnd - Math.floor((viewRegEnd-viewRegStart)/4));
    render(viewRefName, newStart, newEnd);
  }
}

// Try to move right
function moveRight() {
 if (validRegion()) {
   var newStart = viewRegStart + Math.floor((viewRegEnd-viewRegStart)/4);
   var newEnd = viewRegEnd + Math.floor((viewRegEnd-viewRegStart)/4);
   render(viewRefName, newStart, newEnd);
 }
}

// Try to move far right
function moveFarRight() {
  if (validRegion()) {
    var newStart = viewRegStart + Math.floor((viewRegEnd-viewRegStart)/2);
    var newEnd = viewRegEnd + Math.floor((viewRegEnd-viewRegStart)/2);
    render(viewRefName, newStart, newEnd);
  }
}

// Try to move very far right
function moveVeryFarRight() {
  if (validRegion()) {
    var newStart = viewRegStart + (viewRegEnd-viewRegStart);
    var newEnd = viewRegEnd + (viewRegEnd-viewRegStart);
    render(viewRefName, newStart, newEnd);
  }
}

// Try to zoom in
function zoomIn() {
  if (validRegion()) {
    var newStart = viewRegStart + Math.floor((viewRegEnd-viewRegStart)/4);
    var newEnd = viewRegEnd - Math.floor((viewRegEnd-viewRegStart)/4);
    render(viewRefName, newStart, newEnd);
  }
}

// Try to zoom out
function zoomOut() {
  if (validRegion()) {
    var newStart = Math.max(0, viewRegStart - Math.floor((viewRegEnd-viewRegStart)/2));
    var newEnd = viewRegEnd + Math.floor((viewRegEnd-viewRegStart)/2);
    render(viewRefName, newStart, newEnd);
  }

}

/* Validates whether form query has defined start, end and name elements
 * @return true if valid elements, false if invalid elements
 */
function validRegion() {
  if (typeof viewRegStart == "undefined" || typeof viewRegEnd == "undefined" || typeof viewRefName == "undefined")
    return false
  else return true
}
