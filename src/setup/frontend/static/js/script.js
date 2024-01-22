/* GENERAL  FUNCTIONS  */

//If there are related research in first research, show it 
function showFirstRelatedResearch(){ 
  table = document.getElementById("ResearchTable");
  rows = table.getElementsByTagName("tr");
  inputRelatedResearch = rows[0].getElementsByTagName("td")[17].textContent;
  detailRelatedResearch = document.getElementById("DetailRelated");
  if(inputRelatedResearch != "") {
    detailRelated.classList.remove('hidden');
    detailRelated.classList.add('active');
  }
}

// Handles new page loads
function onload(){
  selectTableFirstRow();
  populateSearchWithUrlParam();
  KeyboardEvent();
  showFirstRelatedResearch();
}

//Shows all articles after clearing filters
function showAllArticles() {
  table = document.getElementById("ResearchTable");
  rows = table.getElementsByTagName("tr");
  for (var row of rows) { // `for...of` loops through the NodeList
    row.style.display = ""; // shows this row
  }
  $(document).ready(function() {
    $("#ResearchTable tr:visible:first td:nth-child(1)").trigger('click');
  });
}

/* URL FUNCTIONS  */

//Function to Populate URL with search bar
function PopulateUrl(input_text) {
  // Remove extra spaces
  input_text = input_text.trim()

  if (input_text != "") {
    // Construct URLSearchParams object instance from current URL querystring.
    var queryParams = new URLSearchParams(window.location.search);
    
    // Set new or modify existing parameter value. 
    queryParams.set("q", input_text);

    // Replace current querystring with the new one.
      history.replaceState(null, null, "?"+queryParams.toString());
  }
}

//Function to Populate URL with filters
function PopulateUrlWithFilters(segment_text, author_text, region_text, type_text, cluster_text) {
  // Construct URLSearchParams object instance from current URL querystring.
  var queryParams = new URLSearchParams(window.location.search);
  
  // Set new or modify existing parameter value. 
  queryParams.set("s", segment_text);
  queryParams.set("a", author_text);
  queryParams.set("r", region_text);
  queryParams.set("t", type_text);
  queryParams.set("c", cluster_text);

  // Replace current querystring with the new one.
    history.replaceState(null, null, "?"+queryParams.toString());
}

// Gets URL Parameters to populate Search Bar
function getUrlParams() {
  var paramMap = {};
  if (location.search.length == 0) {
    return paramMap;
  }
  var parts = location.search.substring(1).split("&");

  for (var i = 0; i < parts.length; i ++) {
    var component = parts[i].split("=");
    paramMap [decodeURIComponent(component[0])] = decodeURIComponent(component[1]);
  }
  return paramMap;
}

// Populates Search Bar/ Filters with URL Parameters
function populateSearchWithUrlParam() {
  var params = getUrlParams();
    if (location.search.indexOf('q=')>=0) {
      var string = decodeURI(params.q); // converts url to string
      var clean_string = string.replaceAll("+", " ");
      document.getElementById("SearchInput").value = clean_string; 
    } 
}

/* SEARCH BAR FUNCTIONS  */

//function to simulate pressing the enter key (to automatically filter the search bar when clicking on the top tags buttons)
function KeyboardEvent() {
  var keyboardEvent = document.createEvent("KeyboardEvent");
  var initMethod = typeof keyboardEvent.initKeyboardEvent !== 'undefined' ? "initKeyboardEvent" : "initKeyEvent";
  
  keyboardEvent[initMethod](
    "keyup", // event type: keydown, keyup, keypress
    true,      // bubbles
    true,      // cancelable
    window,    // view: should be window
    false,     // ctrlKey
    false,     // altKey
    false,     // shiftKey
    false,     // metaKey
    13,        // keyCode: 13 corresponds to enter key
    13          // charCode: 13 corresponds to enter key
  );
  document.getElementById("SearchInput").dispatchEvent(keyboardEvent);
}

//Function to filter research pane articles based on search bar input
function SearchFunction() {
  // Declare variables
  var input, count_text, filter, table, tr, td_title, td_author, td_tags, i, txtValue, count;
  input = document.getElementById("SearchInput");
  count_text = document.getElementById("CountText");
  filter = input.value.toUpperCase();
  table = document.getElementById("ResearchTable");
  tr = table.getElementsByTagName("tr");
  count = 0;

  // Loop through all table rows, and hide those who don't match the search query
  for (i = 0; i < tr.length; i++) {
    td_title = tr[i].getElementsByTagName("td")[2]; // Index 2 corresponds to titles
    td_author = tr[i].getElementsByTagName("td")[3]; // Index 3 corresponds to authors
    td_tags = tr[i].getElementsByTagName("td")[4]; // Index 4 corresponds to tags

    if (td_title, td_author, td_tags) {
      txtValue = (td_title.textContent || td_title.innerText) + (td_author.textContent || td_author.innerText) + (td_tags.textContent || td_tags.innerText);
      if (txtValue.toUpperCase().indexOf(filter) > -1) {
        tr[i].style.display = "";
        count = count + 1
        // Ensure First visible row is the one always shown
        $(document).ready(function() {
          $("#ResearchTable tr:visible:first td:nth-child(1)").trigger('click');
        });
      } else {
        tr[i].style.display = "none";
      }
    }
  }
  count_text.innerHTML = "Articles found: " + count + " out of " + tr.length; // Populate Articles Found Text
}

//Populate search bar with clicked tag
function PopulateInput(tag) {
  document.getElementById("SearchInput").value = tag;
  KeyboardEvent();
}

//Runs all search functions
function onSearch()  {
  input = document.getElementById("SearchInput");
  PopulateUrl(input.value);
  SearchFunction();
}


/* FILTERS FUNCTIONS  */

//Function to filter research pane articles based on filters input
function filterTable() {
  // Variables
  document.getElementById("SearchInput").value = '';
  var count_text, dropdown_author, dropdown_region, dropdown_clusters, filter_author, dropdown_segment, dropdown_research_type;
  var filter_region, filter_clusters, filter_segment, filter_research_type, table, rows;
  var cells_clusters, cells_region, cells_author, cells_segment, cells_research_type, count;
  count_text = document.getElementById("CountText");
  dropdown_author = document.getElementById("authorsDropdown");
  dropdown_region = document.getElementById("regionDropdown");
  dropdown_clusters = document.getElementById("clustersDropdown");
  dropdown_segment = document.getElementById("segmentDropdown");
  dropdown_research_type = document.getElementById("researchtypeDropdown");
  filter_author = dropdown_author.value;
  filter_region = dropdown_region.value;
  filter_clusters = dropdown_clusters.value;
  filter_segment = dropdown_segment.value;
  filter_research_type = dropdown_research_type.value;
  table = document.getElementById("ResearchTable");
  rows = table.getElementsByTagName("tr");
  count = 0;

  // Loops through rows and hides those with countries that don't match the filter
  for (var row of rows) { // `for...of` loops through the NodeList
  cells_clusters = row.getElementsByTagName("td")[16] || null;
  cells_region = row.getElementsByTagName("td")[5] || null;
  cells_author = row.getElementsByTagName("td")[3] || null;
  cells_segment = row.getElementsByTagName("td")[11] || null;
  cells_research_type = row.getElementsByTagName("td")[12] || null;
  // if the filter is set to 'All', or this is the header row, or 2nd `td` text matches filter
  if  ((filter_segment == "All Segments" || cells_segment.innerText.indexOf(filter_segment) > -1) && (filter_research_type == "All Types" || cells_research_type.innerText.indexOf(filter_research_type) > -1) && (filter_clusters == "All Research Clusters" || cells_clusters.innerText.indexOf(filter_clusters) > -1) && (filter_author == "All Authors" || cells_author.innerText.indexOf(filter_author) > -1) && (filter_region == "All Regions" || cells_region.innerText.indexOf(filter_region) > -1)){
    row.style.display = ""; // shows this row
    count = count + 1
    // Ensure First visible row is the one always shown
    $(document).ready(function() {
      $("#ResearchTable tr:visible:first td:nth-child(1)").trigger('click');
    });
  }
  else {
    row.style.display = "none"; // hides this row
    }
  }  
  count_text.innerHTML = "Articles found: " + count + " out of " + rows.length; // Populate Articles Found Text
  PopulateUrl(''); // Set url to blank
  //PopulateUrlWithFilters(filter_segment, filter_author, filter_region, filter_research_type, filter_clusters); // Set url to filters once we can pupulate filters with URL
  document.getElementById("ClearFilterButton").disabled = false; //Enable Clear All Filter after having enabled filter
}


/* RESEARCH PANE FUNCTIONS  */
function selectTableFirstRow() {
  $(document).ready(function() {
    $("#ResearchTableRow").click();
  });
  //sets cursor on the search input
  document.getElementById("SearchInput").focus();
  document.getElementById("SearchInput").select();
};




/* CONTENT PANE FUNCTIONS  */
// Populate Content Pane with content from invisible columns from research_df
$(document).ready(function() {
  $('td').click(function() {
    var detailRegion, detailTitle, detailAuthor, detailDate, detailTags, detailSummary, detailLink, detailGithub, detailSegment, detailResearchType, detailHtml, detailRelatedResearch;    var inputRegion, inputTitle, inputAuthor, inputDate, inputTags, inputSummary, inputLink, inputLinkIcon, inputGithub, inputTagsList, inputHtml, inputHtmlIcon, inputGithubIcon;
    table = document.getElementById("ResearchTable");
    rows = table.getElementsByTagName("tr");

    //Get text fields to be populated
    detailRegion = document.getElementById("DetailRegion");
    detailTitle = document.getElementById("DetailTitle");
    detailAuthor = document.getElementById("DetailAuthor");
    detailDate = document.getElementById("DetailDate");
    detailTags = document.getElementById("DetailTagsList");
    detailSummary = document.getElementById("DetailSummary");
    detailDate = document.getElementById("DetailDate");
    detailLink = document.getElementById("DetailLink");
    detailGithub = document.getElementById("DetailGithub");
    detailSegment = document.getElementById("DetailSegment");
    detailResearchType = document.getElementById("DetailResearchType");
    detailHtml = document.getElementById("DetailHtml");
    detailRelatedResearch = document.getElementById("DetailRelatedList");
    detailRelated = document.getElementById("DetailRelated");

    //Get content from invisible columns to populate Fields
    var row = $(this).parent().parent().children().index($(this).parent());
    inputTitle = rows[row].getElementsByTagName("td")[2].textContent;
    inputAuthor = rows[row].getElementsByTagName("td")[3].textContent;
    inputRegion = rows[row].getElementsByTagName("td")[5].textContent;
    inputTags = rows[row].getElementsByTagName("td")[4].textContent;
    inputDate = rows[row].getElementsByTagName("td")[6].textContent;
    inputSummary = rows[row].getElementsByTagName("td")[7].textContent;
    inputLink = rows[row].getElementsByTagName("td")[8].textContent;
    inputLinkIcon = rows[row].getElementsByTagName("td")[9].textContent;
    inputGithub = rows[row].getElementsByTagName("td")[10].textContent;
    inputSegment = rows[row].getElementsByTagName("td")[11].textContent;
    inputResearchType = rows[row].getElementsByTagName("td")[12].textContent;
    inputHtml = rows[row].getElementsByTagName("td")[13].textContent;
    inputHtmlIcon = rows[row].getElementsByTagName("td")[14].textContent;
    inputGithubIcon = rows[row].getElementsByTagName("td")[15].textContent;
    inputRelatedResearch = rows[row].getElementsByTagName("td")[17].textContent;
    inputTagsList = inputTags.split(', ');
    inputRelatedList = inputRelatedResearch.split(', ');

    //Populate fields with text
    detailRegion.innerHTML = inputRegion;
    detailTitle.innerHTML = inputTitle;
    detailAuthor.innerHTML = inputAuthor;
    detailDate.innerHTML = inputDate;
    detailSummary.innerHTML = inputSummary;
    detailGithub.innerHTML = '<a href=' + inputGithub + '><img src="images/github-32.png" /></a>';
    detailSegment.innerHTML = inputSegment;
    detailResearchType.innerHTML = inputResearchType;
    detailTags.innerHTML = '';
    detailRelatedResearch.innerHTML = '';

    // Hide HTML button if there is no valid src
    if (inputHtmlIcon != 'images/file-html-32.png') {
      detailHtml.innerHTML = ''
    } else {
      detailHtml.innerHTML = '<a href=' + inputHtml + ' target="_blank"><img src='+ inputHtmlIcon +' /></a>';
    }

    // Hide link button if there is no valid src
    if (inputLinkIcon == '"//:0"') {
      detailLink.innerHTML = ''
    } else {
      detailLink.innerHTML = '<a href=' + inputLink + ' target="_blank"><img src='+ inputLinkIcon +' /></a>';
    }

    // Hide github button if there is no valid src
    if (inputGithubIcon == '"//:0"') {
      detailGithub.innerHTML = ''
    } else {
      detailGithub.innerHTML = '<a href=' + inputGithub + ' target="_blank"><img src='+ inputGithubIcon +' /></a>';
    }
    
    // Populate each tag with preset value, if value is none then hide tags
    if(inputTagsList != "") {
      for (i = 0; i < inputTagsList.length; i++) {
        DetailTagsList.style.visibility = 'visible';
        input = "'"+inputTagsList[i]+"'";
        DetailTagsList.innerHTML+='<button class="tag" onclick="PopulateInput('+input+' )">' + inputTagsList[i] + '</button>';  
      }
    } else {
      detailTagsList.style.visibility = 'hidden';
    }

    // Populate each related research with preset value, if value is none then hide related research
    if(inputRelatedResearch != "") {
      detailRelated.classList.add('active');
      detailRelated.classList.remove('hidden');
      for (i = 0; i < inputRelatedList.length; i++) {
        input = "'"+inputRelatedList[i]+"'";
        detailRelatedResearch.innerHTML+='<button class="related_article" onclick="PopulateInput('+input+' )">' + inputRelatedList[i] + '</button>';  
      }
    } else {
      detailRelated.classList.remove('active');
      detailRelated.classList.add('hidden');
    }
    
  });
});