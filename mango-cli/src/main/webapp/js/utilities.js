function getAlignmentSelector(sample) {
    var selector = "#" + sample + ">.alignmentData";
    return selector;
}

function setGlobalReferenceRegion(refName, start, end) {
    viewRefName = refName;
    viewRegStart = start;
    viewRegEnd = end;
}

function isValidHttpResponse(data) {
  if (data == undefined) {
    return false;
  } else {
    return true;
  }
}

// Toggles alignment record contained in the selector for a given sample
function toggleAlignments(sample, selector) {
    var selector = $(getAlignmentSelector(filterName(sample)));
    if (!selector.is(':visible')) {
            renderAlignments(viewRefName, viewRegStart, viewRegEnd, sample);
        }
        $(selector).slideToggle( "fast" );
}

// Filters invalid characters from string to create javascript descriptor
function filterNames(arr) {
  var filteredArr = [];
  for (var i = 0; i < arr.length; i++) {
    filteredArr[i] = arr[i].replace("/","");
  }
  return filteredArr;
}

function filterName(name) {
  return name.replace("/","");
}

Array.prototype.contains = function(v) {
  for(var i = 0; i < this.length; i++) {
      if(this[i] === v) return true;
  }
  return false;
};

Array.prototype.unique = function() {
    var arr = [];
    for(var i = 0; i < this.length; i++) {
        if(!arr.contains(this[i])) {
            arr.push(this[i]);
        }
    }
    return arr;
};


function checkboxChange() {
  if (indelCheck.checked) {
    $(".indel").show();

  } else {
    $(".indel").hide();
  }

  if (mismatchCheck.checked) {
    $(".mrect").show();
  } else {
    $(".mrect").hide();
  }

  if (coverageCheck.checked) {
    $(".coverage-svg").show();
  } else {
    $(".coverage-svg").hide();
  }
}

function toggleContent(validContent) {
    if (validContent) {
        $("#home").css("display", "none");
        $("#tracks").css("display", "block");
    } else {
        $("#home").css("display", "block");
        $("#tracks").css("display", "none");
    }
}

    var re = /(?:\.([^.]+))?$/;

// Upload new file
$("#loadFile:file").change(function(){
  var filename = $("#loadFile:file").val();
  var ext = re.exec(filename)[1];

  if (ext == "bam" || ext == "vcf" || ext == "adam") {
    samples.push(filename);
  }

});

// Upload new reference file
$("#loadRef:file").change(function(){
  var filename = $("#loadRef:file").val();
});

