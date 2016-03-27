$(document).ready( function() {
        $('#fileTree_1').fileTree({ root: '../', script: '../../resources/connectors/jqueryFileTree.php' }, function(file) { 
          alert(file);
        });        
});