/** @type {Number} Calculate range using viewRegEnd - viewRegStart
    var range = viewRegEnd - viewRegStart */
var varJsonLocation = "/variants/" + viewRefName + "?start=" + viewRegStart + "&end=" + viewRegEnd;
var sampleJsonLocation = "sample.json";
var range = 50;
var gridSize = Math.floor(width / range)
var rangeStart = viewRegStart;
var rangeEnd = viewRegEnd;
var height = $('.heatmap').height();
var width =  $('.heatmap').width();
var refrefFactor;//Green
var refref = 'rgb(3,153,104)';
var refaltFactor; //Blue
var refalt = 'rgb(9,57,153)';
var altaltFactor; //Red
var altalt = 'rgb(255,141,0)';

/** [Description] */
var appendElements = (function(start, end, times) {
  /** Validate that range is an int
      Validate that times is [] */
  for(var x = start; x <= end; x += 1) {
    times.push(x);
  }
  return times;
});

var exactMouse = (function(x, gridSize) {
  var overflow = x % gridSize;
  if (x % gridSize > 0) {
    return x + gridSize / 2;
  } else {
    return x; 
  }
});

function renderHeatMap(refName, start, end) {
  viewRegStart = start;
  viewRegEnd = end;
  viewRefName = refName;
  varJsonLocation = "/variants/" + viewRefName + "?start=" + viewRegStart + "&end=" + viewRegEnd;
  varFreqJsonLocation = "/variantfreq/" + viewRefName + "?start=" + viewRegStart + "&end=" + viewRegEnd;
  renderJsonHeatMap();
}


function renderJsonHeatMap() {
  d3.json(varJsonLocation, function(error, data) {
    if (jQuery.isEmptyObject(data)) {
      return;
    }
    if (error) throw error;
    console.log(data);
    //dynamically setting height of svg containers
    // var numTracks = d3.max(data, function(d) {return d.track});
    // var varTrackHeight = getTrackHeight()
    // varHeight = (numTracks+1)*varTrackHeight;
    // varSvgContainer.attr("height", varHeight);
    // $(".verticalLine").attr("y2", varHeight);

    // Add the rectangles 
    var heatMapVariants = varSvgContainer.selectAll(".variant").data(data);
    var modify = heatMapVariants.transition();
    modify
      .attr("x", (function(d) { return (d.start-viewRegStart)/(viewRegEnd-viewRegStart) * width; }))
      .attr("width", (function(d) { return Math.max(1,(d.end-d.start)*(width/(viewRegEnd-viewRegStart))); }));
    var heatMapData = heatMapVariants.enter();


    var calcX = (function(d) {
      return d.start;
    });

    var calcRefRefY = (function(d) {
      return d.personNum * gridSize;
    });

    var calcColor = (function(d) {
      if (d.alleles == "Ref / Alt") {
        return refalt;
      } else if (d.alleles == "Alt / Alt") {
        return altalt;
      } else if (d.alleles == "Ref / Ref") {
        return refref;
      } else {
        return null;
      }
    });

    var varInfo = d3.select("#varInfo")
    .append("div")
    .attr("class", "tooltip")
    .style("opacity", 0);

    var triangle = d3.select('.triangle');

    var varHeatMapEnter = svgHeat.selectAll(".hour").data(data);
    varHeatMapEnter.enter().append("rect")
              .attr("x", function(d) { return (d.start - 1) * gridSize; })
              .attr("y", function(d) { return 0}) //adjust 0 
              .attr("rx", 4)
              .attr("ry", 4)
              .attr("class", "hour bordered")
              .attr("width", gridSize)
              .attr("height", gridSize)
              .text(function(d) {return d.alleles;})
              .style("fill", function(d) {
                              if (d.alleles == "Ref / Alt") {
                                return refalt;
                              } else if (d.alleles == "Alt / Alt") {
                                return altalt;
                              } else if (d.alleles == "Ref / Ref") {
                                return refref;
                              } else {
                                return null;
                              }
                            })
              .on('mouseover', (function(d) {
                  varInfo.transition()
                    .duration(200)
                    .style("opacity", 1);
                  varInfo.html(d.alleles)
                    .style("position", "relative")
                    // .attr("x", function(d) { return (d.start - 1) * gridSize; })
                    // .attr("y", function(d) { return 0})
                    .style("left", exactMouse(d3.mouse(this)[0], gridSize) +  "px")
                    .style("top", "-90px")
                    .style("width", "100px")
                    .style("text-align", "center")
                    .attr("class", "d3-tip");
                  triangle
                    .style("left", exactMouse(d3.mouse(this)[0], gridSize) + 40 + "px")
                    .style("top", "-70px");
                  triangle.transition()
                    .duration(300)
                    .style("opacity", 1);
                }))
              .on('mouseout', (function(d) {
                varInfo.transition()
                .duration(200)
                .style("opacity", 0);
                triangle.transition()
                .duration(100)
                .style("opacity", 0);
              }));



    // cards.transition().duration(1000)
    //           .style("fill", function(d) { return color(1); });

      var varHeatMapChange = varHeatMapEnter.transition()
        .duration(300)
        .attr("x", function(d) { return (d.start - 1) * gridSize; })
              .attr("y", function(d) { return 0})
              .attr("rx", 4)
              .attr("ry", 4)
              .attr("class", "hour bordered")
              .attr("width", gridSize)
              .attr("height", gridSize)
              .text(function(d) {return d.alleles;})
              .style("fill", function(d) {
                              if (d.alleles == "Ref / Alt") {
                                return refalt;
                              } else if (d.alleles == "Alt / Alt") {
                                return altalt;
                              } else if (d.alleles == "Ref / Ref") {
                                return refref;
                              } else {
                                return null;
                              }
                            });

      var removedMap = varHeatMapEnter.exit();
        removedMap.remove();
  });
}

var margin = { top: 30, right: 60, bottom: 100, left: 15 },
          // width = 960 - margin.left - margin.right,
          // height = 430 - margin.top - margin.bottom,
          width = width - margin.left;
          gridSize = Math.floor(width / range),
          legendElementWidth = gridSize*2,
          buckets = 3,
          colors = ["#ffffd9","#edf8b1","#c7e9b4","#7fcdbb","#41b6c4","#1d91c0","#225ea8","#253494","#081d58"], // alternatively colorbrewer.YlGnBu[9]
          people = ["1", "2", "3"],
          times = appendElements(viewRegStart, viewRegEnd, []);
          datasets = [];

var svgHeat = d3.select("#chart").append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var dayLabels = svgHeat.selectAll(".dayLabel")
    .data(people)
    .enter().append("text")
      .text(function (d) { return d; })
      .attr("x", 0)
      .attr("y", function (d, i) { return i * gridSize; })
      .style("text-anchor", "end")
      .attr("transform", "translate(-6," + gridSize / 1.5 + ")")
      .attr("class", function (d, i) { return ((i >= 0 && i <= 4) ? "dayLabel mono axis axis-workweek" : "dayLabel mono axis"); });

var timeLabels = svgHeat.selectAll(".timeLabel").data(times);

var timeLabelsEnter = timeLabels.enter().append("text")
      .text(function(d) { return d; })
      .attr("x", function(d, i) { return i * gridSize; })
      .attr("y", 0)
      .style("text-anchor", "middle")
      .attr("transform", "translate(" + gridSize / 2 + ", -6)")
      .attr("class", function(d, i) { return ((i >= 7 && i <= 16) ? "timeLabel mono axis axis-worktime" : "timeLabel mono axis"); });


var removeTimeLabels = timeLabels.exit();
removeTimeLabels.remove();

var updateTimeLabels = timeLabels.transition()
  updateTimeLabels
  .transition(300)
  // .data(appendElements(viewRegStart, viewRegEnd, []))
  // .enter().append("text")
  // .text(function(d) { return d; })
  .attr("x", function(d, i) { return i * gridSize; })
  .attr("y", 0)
  .style("text-anchor", "middle")
  .attr("transform", "translate(" + gridSize / 2 + ", -6)")
  .attr("class", function(d, i) { return ((i >= 7 && i <= 16) ? "timeLabel mono axis axis-worktime" : "timeLabel mono axis"); });

function renderHeatMapLabels(data, scale) {
  var refString = scale.selectAll(".bases")
                  .data(data);

  var modify = refString.transition();
  modify
      .attr("x", 0)
      .attr("dx", function(d, i) {
           return i/(viewRegEnd-viewRegStart) * refWidth - (refWidth/(viewRegEnd-viewRegStart))/2 ;
      })
      .text( function (d) { return d.reference; })
      .attr("fill", function(d) {
        if (d.reference === "N") return nColor;
        else return baseColors[d.reference];
      });

  var newData = refString.enter();
  newData
      .append("text")
      .attr("class", "bases")
      .attr("y", 30)
      .attr("x", 0)
      .attr("dx", function(d, i) {
        return i/(viewRegEnd-viewRegStart) * refWidth - (refWidth/(viewRegEnd-viewRegStart))/2 ;
          })
      .text( function (d) { return d.reference; })
      .attr("font-family", "Sans-serif")
      .attr("font-weight", "bold")
      .attr("font-size", "12px")
      .attr("fill", function(d) {
        if (d.reference === "N") return nColor;
        else return baseColors[d.reference];
      });

    var removed = refString.exit();
    removed.remove();
}

// renderHeatMap(viewRefName, viewRegStart, viewRegEnd);
