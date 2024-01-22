/*
Reference: http://jsfiddle.net/BB3JK/47/
*/

// Iterate over each select element
$('select').each(function(){
    // Cache the number of options
    var $this = $(this), numberOfOptions = $(this).children('option').length;

    // Hides the select element
    $this.addClass('select-hidden'); 
    // Wrap the select element in a div
    $this.wrap('<div class="select" oninput="filterTable()"></div>');
    // Insert a styled div to sit over the top of the hidden select element
    $this.after('<div class="select-styled"></div>');
  
    // Cache the styled div
    var $styledSelect = $this.next('div.select-styled');
    // Show the first select option in the styled div
    $styledSelect.text($this.children('option').eq(0).text());

    // Insert an unordered list after the styled div and also cache the list
    var $list = $('<ul />', {
        'class': 'select-options'
    }).insertAfter($styledSelect);

    // Insert a list item into the unordered list for each select option
    for (var i = 0; i < numberOfOptions; i++) {
        //'class': 'select-item',
        $('<li />', {
            text: $this.children('option').eq(i).text(),
            rel: $this.children('option').eq(i).val()
        }).appendTo($list);
    }

    // Cache the list items
    var $listItems = $list.children('li');
  
    // Show the unordered list when the styled div is clicked (also hides it if the div is clicked again)
    $styledSelect.click(function(e) {
        e.stopPropagation();
        $('div.select-styled.active').not(this).each(function(){
            $(this).removeClass('active').next('ul.select-options').hide();
        });
        $(this).toggleClass('active').next('ul.select-options').toggle();
    });

    // Hides the unordered list when a list item is clicked and updates the styled div to show the selected list item
    // Updates the select element to have the value of the equivalent option
    $listItems.click(function(e) {
        e.stopPropagation(); //prevents propagation of the same event from being called
        $styledSelect.text($(this).text()).removeClass('active'); //main text is no longer acitve
        $this.val($(this).attr('rel')); 
        //Keep child text active
        $list.hide(); // hides list
        $(function () {
            filterTable(); // added filter function here
          });

    });

    // Hides the unordered list when clicking outside of it
    $(document).click(function() {
        $styledSelect.removeClass('active');
        $list.hide();
    });

    // resets filters when clicking on one of the tags buttons
    var tags = $('#DetailTagsList')[0];
    var related_research = $('#DetailRelatedList')[0];
    $(tags).add(related_research).on('click', function() {
        $styledSelect.removeClass('active');
        $list.hide();
        $styledSelect.text($this.children('option').eq(0).text()); //sets text to first value
        $this.val($(this).attr('rel')); 
    });

    // resets filters when clicking on the search bar
    var search_bar = $('#SearchInput')[0];
    $(search_bar).click(function() {
        $styledSelect.removeClass('active');
        $list.hide();
        $styledSelect.text($this.children('option').eq(0).text()); //sets text to first value
        $this.val($(this).attr('rel')); 
        showAllArticles(); //Shows all articles
        document.getElementById("ClearFilterButton").disabled = true; // disable clear all filters button
    });

    // resets filters when clicking on the Clear All filters button/ Header Logo
    var clear_filters = $('#ClearFilterButton')[0];
    var header_logo = $('#HeaderLogo')[0];
    $(clear_filters).add(header_logo).on('click', function() {
        $styledSelect.removeClass('active');
        $list.hide();
        $styledSelect.text($this.children('option').eq(0).text()); //sets text to first value
        $this.val($(this).attr('rel')); 
        document.getElementById("SearchInput").value = '';  //Clears Search Bar
        showAllArticles(); //Shows all articles
        window.history.pushState({}, document.title, "/"); //Resets URL to default
        document.getElementById("ClearFilterButton").disabled = true; // disable clear all filters button
    });


});
