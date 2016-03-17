function SliderSetup()
  {
      $("#slider").slider({
              value:50,
              min: 0,
              max: 100,
              step: 1,
              slide: function(event, ui) {
                  setTimeout(showVal, 10);
              }
          });    
      
  }

function showVal() {
    var slider = $('.ui-slider-handle:first');
    var position = slider.offset();
    var value = $('#slider').slider('value');
    var val = $('#value1');
    val.text(value).css({'left':position.left + ((slider.width() - val.width()) /2), 'top':position.top -30 });
}     

$(function(){  
SliderSetup();
});