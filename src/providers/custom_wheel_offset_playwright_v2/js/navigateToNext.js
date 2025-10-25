// ═══════════════════════════════════════════════════════════════════
// YMM DROPDOWN NAVIGATION FLOW LOGIC (Extended with 3 Optional Dropdowns)
// ═══════════════════════════════════════════════════════════════════

// Persisted object to store current dropdown li references
const persisted = {
  years: [],
  makes: [],
  models: [],
  trims: [],
  drives: [],
  suspensions: [],
  modifications: [],
  rubbings: []
};

// Current selection tracking
let currentSelection = {
  year: null,
  make: null,
  model: null,
  trim: null,
  drive: null,
  suspension: null,
  modification: null,
  rubbing: null
};

// Global flag to determine if we should handle extra 3 dropdowns
let handleExtraDropdowns = false;

// Global variable to store the minimum year limit
let yearLimit = null;

// ───────────────────────────────────────────────────────────────────
// HELPER FUNCTIONS
// ───────────────────────────────────────────────────────────────────

/**
 * Remove unnecessary "All Makes" entries from make list
 */
function removeUnnecessaryMakes(allMakeLi) {
  const i = allMakeLi.findIndex(makeLi => 
    makeLi.innerText.toLowerCase().trim() === "all makes"
  );
  if (i >= 0) {
    allMakeLi.splice(0, i + 1);
  }
}

/**
 * Get current list of li elements from DOM for a given dropdown type
 */
function getListFromDOM(type) {
  let selector;
  
  // Map type to the correct selector
  if (type === 'suspension') {
    selector = '.store-ymm-drop .ymm-selected[data-type="suspension"]';
  } else if (type === 'modification') {
    selector = '.store-ymm-drop .ymm-selected[data-type="mod"]';
  } else if (type === 'rubbing') {
    selector = '.store-ymm-drop .ymm-selected[data-type="rub"]';
  } else {
    selector = `#${type}`;
  }
  
  const dropdown = document.querySelector(selector);
  if (!dropdown) {
    // console.warn(`[LIST] Dropdown selector for '${type}' not found.`);
    return [];
  }
  
  const listContainer = dropdown.closest('.store-ymm-drop');
  if (!listContainer) {
    // console.warn(`[LIST] List container for '${type}' not found.`);
    return [];
  }
  
  let items = Array.from(listContainer.querySelectorAll('li.ymm-li'));
  
  // Clean up makes if needed
  if (type === 'make' && items.length > 0) {
    removeUnnecessaryMakes(items);
  }
  
  // --- COMPREHENSIVE LOGGING ---
  // console.log(`[LIST] Available items for '${type}':`, items.map(li => li.textContent.trim()));
  
  return items;
}

/**
 * Wait until the list updates (DOM li references change) OR until a static list is stable.
 * CRITICAL: This function is now robust enough to handle both dynamic and static lists.
 * @param {HTMLLIElement[]} oldList - The previous list of <li> elements.
 * @param {string} type - The dropdown type to check.
 * @param {number} timeout - Max time to wait.
 * @param {boolean} allowStaticResolution - If true, will resolve after 2s of no change.
 */
function waitForListUpdate(oldList, type, timeout = 10000, allowStaticResolution = false) {
  return new Promise((resolve, reject) => {
    const startTime = Date.now();
    let lastChangeTime = Date.now();
    
    // console.log(`[WAITING] Waiting for '${type}' list to update... (Static Resolution: ${allowStaticResolution})`);
    // console.log(`[DEBUG] Old '${type}' list length: ${oldList.length}`);
    
    const checkInterval = setInterval(() => {
      const currentList = getListFromDOM(type);
      
      // Check if lists are different (by reference comparison)
      const hasChanged = (!oldList.every(item => currentList.includes(item)) || 
                        oldList.length !== currentList.length) &&
                        currentList.length > 0; // Ensure we have items
      
      if (hasChanged) {
        clearInterval(checkInterval);
        // console.log(`[UPDATE DETECTED] '${type}' list has been updated. New list length: ${currentList.length}`);
        setTimeout(() => resolve(currentList), 500);
        return;
      }

      // --- LOGIC for static lists ---
      if (allowStaticResolution && currentList.length > 0) {
        // If the list is populated and hasn't changed for 2 seconds, assume it's static.
        if (Date.now() - lastChangeTime > 2000) {
          clearInterval(checkInterval);
          // console.log(`[STATIC RESOLUTION] '${type}' list did not change after 2s. Assuming it's static and resolving.`);
          setTimeout(() => resolve(currentList), 500);
          return;
        }
      } else {
        // Reset the timer if the list is empty or we're not allowing static resolution
        lastChangeTime = Date.now();
      }
      
      // Timeout check
      if (Date.now() - startTime > timeout) {
        clearInterval(checkInterval);
        // console.error(`[ERROR] Timeout waiting for '${type}' list update.`);
        reject(new Error(`Timeout waiting for ${type} list update`));
      }
    }, 400);
  });
}

/**
 * Open a dropdown by clicking on it
 */
function openDropdown(type) {
  let selector;
  
  // Map type to the correct selector
  if (type === 'suspension') {
    selector = '.store-ymm-drop .ymm-selected[data-type="suspension"]';
  } else if (type === 'modification') {
    selector = '.store-ymm-drop .ymm-selected[data-type="mod"]';
  } else if (type === 'rubbing') {
    selector = '.store-ymm-drop .ymm-selected[data-type="rub"]';
  } else {
    selector = `#${type}`;
  }
  
  const element = document.querySelector(selector);
  if (!element) {
    throw new Error(`Dropdown ${type} not found`);
  }
  
  const dropdownContainer = element.closest('.store-ymm-drop');
  if (!dropdownContainer) {
    throw new Error(`Dropdown container for ${type} not found`);
  }
  
  dropdownContainer.click();
  // console.log(`[OPEN] '${type}' dropdown opened`);
}

/**
 * Select an item by text value
 * IMPORTANT: Does NOT wait for dependent dropdown updates
 */
async function selectItemByText(type, text) {
  // console.log(`\n[ACTION] Attempting to select in '${type}': "${text}"`);
  openDropdown(type);
  
  // --- ROBUST RETRY MECHANISM ---
  let items = [];
  const maxRetries = 5;
  for (let i = 0; i < maxRetries; i++) {
    await sleep(300); // Wait for dropdown to render
    items = getListFromDOM(type); // This will log the available items
    
    if (items.length > 0) {
      break; // List is populated, exit retry loop
    }
    
    // console.warn(`[RETRY] '${type}' list is empty on attempt ${i + 1}/${maxRetries}. Retrying...`);
  }

  if (items.length === 0) {
    // console.error(`[ERROR] Item "${text}" not found in '${type}' dropdown after ${maxRetries} retries.`);
    // console.log(`[DEBUG] Available items were:`, items.map(li => li.textContent.trim()));
    throw new Error(`Item "${text}" not found in ${type} dropdown after retries`);
  }
  
  const searchText = text.toLowerCase().trim();
  const targetItem = items.find(li => 
    li.textContent.toLowerCase().trim() === searchText
  );
  
  if (!targetItem) {
    // console.error(`[ERROR] Item "${text}" not found in '${type}' dropdown.`);
    // console.log(`[DEBUG] Available items were:`, items.map(li => li.textContent.trim()));
    throw new Error(`Item "${text}" not found in ${type} dropdown`);
  }
  
  targetItem.click();
  // console.log(`[SELECT] Successfully selected '${type}': "${text}"`);
  currentSelection[type] = targetItem;
  
  return targetItem;
}

/**
 * Select the first item in a dropdown
 * IMPORTANT: Does NOT wait for dependent dropdown updates
 */
async function selectFirstItem(type) {
  // console.log(`\n[ACTION] Attempting to select FIRST item in '${type}'`);
  openDropdown(type);

  // --- ROBUST RETRY MECHANISM ---
  let currentList = [];
  const maxRetries = 5;
  for (let i = 0; i < maxRetries; i++) {
    await sleep(300); // Wait for dropdown to render
    currentList = getListFromDOM(type); // This will log the available items

    if (currentList.length > 0) {
      break; // List is populated, exit retry loop
    }

    // console.warn(`[RETRY] '${type}' list is empty on attempt ${i + 1}/${maxRetries}. Retrying...`);
  }
  
  if (currentList.length === 0) {
    throw new Error(`No items found in ${type} dropdown after ${maxRetries} retries`);
  }
  
  const firstItem = currentList[0];
  const itemText = firstItem.textContent.trim();
  firstItem.click();
  
  // console.log(`[SELECT FIRST] Successfully selected first '${type}': "${itemText}"`);
  
  currentSelection[type] = firstItem;
  persisted[`${type}s`] = currentList;
  
  return firstItem;
}

/**
 * NEW, MOST RELIABLE FUNCTION to find the next unique item in a list.
 * This correctly handles cases where list items are duplicated in the DOM.
 * @param {HTMLLIElement} currentLi - The currently selected list item.
 * @param {string} type - The dropdown type.
 * @returns {HTMLLIElement|null} The next unique item or null if none exists.
 */
function getNextUniqueItem(currentLi, type) {
  if (!currentLi) return null;
  
  const currentText = currentLi.textContent.trim();
  const allItems = getListFromDOM(type);
  
  // console.log(`[LOGIC] Finding next unique item for '${type}'. Current is "${currentText}".`);
  
  // Create a map to store the first occurrence of each unique item text
  const uniqueItemsMap = new Map();
  for (const item of allItems) {
    const text = item.textContent.trim();
    if (!uniqueItemsMap.has(text)) {
      uniqueItemsMap.set(text, item);
    }
  }
  
  // Convert the map values back to an array to get the unique items in order
  const uniqueItems = Array.from(uniqueItemsMap.values());
  // console.log(`[LOGIC] Unique items for '${type}':`, uniqueItems.map(li => li.textContent.trim()));

  // Find the index of the current item in the unique list
  const currentIndex = uniqueItems.findIndex(li => li.textContent.trim() === currentText);
  
  if (currentIndex === -1) {
    // console.error(`[ERROR] Could not find current item "${currentText}" in its own unique list.`);
    return null;
  }
  
  // Check if there is a next item
  if (currentIndex + 1 < uniqueItems.length) {
    const nextItem = uniqueItems[currentIndex + 1];
    // console.log(`[LOGIC] Found next unique item for '${type}': "${nextItem.textContent.trim()}"`);
    return nextItem;
  }
  
  // console.log(`[LOGIC] No next unique item found for '${type}'. "${currentText}" is the last unique option.`);
  return null;
}


/**
 * Find the currently selected li element for a given type
 * This is crucial for determining the next item correctly
 */
function findCurrentSelectedItem(type) {
  let selector;
  
  // Map type to the correct selector
  if (type === 'suspension') {
    selector = '.store-ymm-drop .ymm-selected[data-type="suspension"]';
  } else if (type === 'modification') {
    selector = '.store-ymm-drop .ymm-selected[data-type="mod"]';
  } else if (type === 'rubbing') {
    selector = '.store-ymm-drop .ymm-selected[data-type="rub"]';
  } else {
    selector = `#${type}`;
  }
  
  const selectedElement = document.querySelector(selector);
  if (!selectedElement) {
    // console.warn(`[CURRENT] Selected element for '${type}' not found.`);
    return null;
  }
  
  const selectedText = selectedElement.textContent.trim();
  const listContainer = selectedElement.closest('.store-ymm-drop');
  const items = Array.from(listContainer.querySelectorAll('li.ymm-li'));
  
  // Find the li element that matches the selected text
  const selectedItem = items.find(li => 
    li.textContent.trim() === selectedText
  );
  
  // console.log(`[CURRENT] Found selected item for '${type}': "${selectedText}"`);
  return selectedItem;
}

/**
 * Sleep utility
 */
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Wait for apply button to become active and click it
 */
async function waitAndClickApply() {
  // console.log('\n[ACTION] Waiting for apply button to become active...');
  
  return new Promise((resolve) => {
    const checkInterval = setInterval(() => {
      const applyButton = document.querySelector('.apply-ymm.active');
      
      if (applyButton) {
        clearInterval(checkInterval);
        // console.log('[FOUND] Apply button is active. Waiting 2s before clicking...');
        
        setTimeout(() => {
          const divInside = applyButton.querySelector('div');
          if (divInside) {
            divInside.click();
            // console.log('[CLICK] Apply button clicked.');
            resolve();
          } else {
            // console.warn('[WARNING] No div found inside apply button');
            resolve();
          }
        }, 2000);
      }
    }, 300);
  });
}

// ───────────────────────────────────────────────────────────────────
// MAIN NAVIGATION LOGIC
// ───────────────────────────────────────────────────────────────────

/**
 * Navigate and apply next combination
 * @param {Object} resumeFrom - Optional: {year, make, model, trim, drive, suspension?, modification?, rubbing?} to resume from.
 * @param {Boolean} includeExtra - Optional: If true, handle extra 3 dropdowns (suspension, modification, rubbing). Default: false.
 * @param {String} startYear - Optional: The specific year to start from when not resuming. Ignored if resumeFrom is provided.
 * @param {String} minYear - Optional: The minimum year to process. Script will stop if the next year is below this limit.
 */
async function navigateAndApplyNext(resumeFrom = null, includeExtra = false, startYear = null, minYear = null) {
  try {
    // Set global flags
    handleExtraDropdowns = includeExtra;
    yearLimit = minYear;
    
    // console.log('\n═══════════════════════════════════════════════════════');
    // console.log(`[MODE] ${includeExtra ? '8 Dropdowns Mode' : '5 Dropdowns Mode'}`);
    // console.log(`[CONFIG] Start Year: ${startYear || 'N/A'}, Min Year: ${minYear || 'N/A'}`);
    
    // Check for a valid resumeFrom object
    if (!resumeFrom || !resumeFrom.year) {
      // console.log('[START] Beginning from a new combination.');
      await selectInitialCombination(startYear, minYear);
    } else {
      // console.log('[RESUME] Resuming from:', resumeFrom);
      await findNextCombination(resumeFrom);
    }
    
    // Wait for apply button and click it
    await waitAndClickApply();
    
    // console.log('[SUCCESS] Combination applied successfully');
    
    const selectionLog = {
      year: currentSelection.year?.textContent.trim(),
      make: currentSelection.make?.textContent.trim(),
      model: currentSelection.model?.textContent.trim(),
      trim: currentSelection.trim?.textContent.trim(),
      drive: currentSelection.drive?.textContent.trim()
    };
    
    if (handleExtraDropdowns) {
      selectionLog.suspension = currentSelection.suspension?.textContent.trim();
      selectionLog.modification = currentSelection.modification?.textContent.trim();
      selectionLog.rubbing = currentSelection.rubbing?.textContent.trim();
    }
    
    // console.log('Current Selection:', selectionLog);
    // console.log('═══════════════════════════════════════════════════════\n');
    
  } catch (error) {
    // Gracefully handle 'STOP' and 'COMPLETE' without re-throwing.
    if (error.message === 'STOP' || error.message === 'COMPLETE') {
      // console.log(`\n[FINISHED] ${error.message}.`);
    } else {
      // For any other unexpected error, log it and re-throw.
      // console.error('[ERROR]', error.message);
      throw error;
    }
  }
}

/**
 * Select the initial combination based on startYear and minYear.
 * @param {String} startYear - The specific year to start from.
 * @param {String} minYear - The minimum year allowed.
 */
async function selectInitialCombination(startYear, minYear) {
  const allYears = getListFromDOM('year');
  let yearToSelect;

  // Priority 1: Use startYear if it's provided
  if (startYear) {
    const foundYearLi = allYears.find(li => li.textContent.trim() === startYear);
    if (foundYearLi) {
      yearToSelect = foundYearLi.textContent.trim();
      // console.log(`[START] Selecting specified start year "${yearToSelect}".`);
    } else {
      // console.error(`[ERROR] Specified start year "${startYear}" not found in the list.`);
      throw new Error(`Start year "${startYear}" not found.`);
    }
  } 
  // Priority 2: Use minYear if startYear is not provided
  else if (minYear) {
    const limitValue = parseInt(minYear);
    const foundYearLi = allYears.find(li => parseInt(li.textContent.trim()) >= limitValue);
    if (foundYearLi) {
      yearToSelect = foundYearLi.textContent.trim();
      // console.log(`[START] Selecting first year "${yearToSelect}" that meets the limit of ${minYear}.`);
    } else {
      // console.log(`[STOP] No year found that meets the limit of ${minYear}.`);
      throw new Error('STOP');
    }
  }
  // Priority 3: Default to the first available year
  else {
    yearToSelect = allYears[0].textContent.trim();
    // console.log(`[START] No start year or limit. Selecting first available year "${yearToSelect}".`);
  }

  await selectItemByText('year', yearToSelect);
  persisted.years = getListFromDOM('year');
  
  const updatedMakes = await waitForListUpdate(persisted.makes, 'make');
  persisted.makes = updatedMakes;
  await selectFirstItem('make');
  
  const updatedModels = await waitForListUpdate(persisted.models, 'model');
  persisted.models = updatedModels;
  await selectFirstItem('model');
  
  const updatedTrims = await waitForListUpdate(persisted.trims, 'trim');
  persisted.trims = updatedTrims;
  await selectFirstItem('trim');
  
  const updatedDrives = await waitForListUpdate(persisted.drives, 'drive');
  persisted.drives = updatedDrives;
  await selectFirstItem('drive');
  
  if (handleExtraDropdowns) {
    const updatedSuspensions = await waitForListUpdate(persisted.suspensions, 'suspension', 10000, true);
    persisted.suspensions = updatedSuspensions;
    await selectFirstItem('suspension');
    
    const updatedModifications = await waitForListUpdate(persisted.modifications, 'modification', 10000, true);
    persisted.modifications = updatedModifications;
    await selectFirstItem('modification');
    
    const updatedRubbings = await waitForListUpdate(persisted.rubbings, 'rubbing', 10000, true);
    persisted.rubbings = updatedRubbings;
    await selectFirstItem('rubbing');
  }
}

/**
 * Find and select the next valid combination
 * PROPERLY WAITS after each selection before continuing
 */
async function findNextCombination(resumeFrom) {
  await selectItemByText('year', resumeFrom.year);
  const updatedMakes = await waitForListUpdate(persisted.makes, 'make');
  persisted.makes = updatedMakes;
  
  await selectItemByText('make', resumeFrom.make);
  const updatedModels = await waitForListUpdate(persisted.models, 'model');
  persisted.models = updatedModels;
  
  await selectItemByText('model', resumeFrom.model);
  const updatedTrims = await waitForListUpdate(persisted.trims, 'trim');
  persisted.trims = updatedTrims;
  
  await selectItemByText('trim', resumeFrom.trim);
  const updatedDrives = await waitForListUpdate(persisted.drives, 'drive');
  persisted.drives = updatedDrives;
  
  await selectItemByText('drive', resumeFrom.drive);
  
  if (handleExtraDropdowns) {
    const updatedSuspensions = await waitForListUpdate(persisted.suspensions, 'suspension', 10000, true);
    persisted.suspensions = updatedSuspensions;
    await selectItemByText('suspension', resumeFrom.suspension);
    
    const updatedModifications = await waitForListUpdate(persisted.modifications, 'modification', 10000, true);
    persisted.modifications = updatedModifications;
    await selectItemByText('modification', resumeFrom.modification);
    
    const updatedRubbings = await waitForListUpdate(persisted.rubbings, 'rubbing', 10000, true);
    persisted.rubbings = updatedRubbings;
    await selectItemByText('rubbing', resumeFrom.rubbing);
  }
  
  await moveToNextCombination();
}

/**
 * Move to the next available combination following the cascade logic
 * PROPERLY WAITS after each dropdown change before selecting dependent dropdowns
 */
async function moveToNextCombination() {
  // console.log('\n[LOGIC] Determining next combination...');
  
  if (handleExtraDropdowns) {
    const currentRubbingItem = findCurrentSelectedItem('rubbing');
    const nextRubbing = getNextUniqueItem(currentRubbingItem, 'rubbing');
    
    if (nextRubbing) {
      // console.log('[NEXT] Moving to next rubbing');
      nextRubbing.click();
      currentSelection.rubbing = nextRubbing;
      return;
    }
    
    // console.log('[LOGIC] No next rubbing. Checking modification...');
    const currentModificationItem = findCurrentSelectedItem('modification');
    const nextModification = getNextUniqueItem(currentModificationItem, 'modification');
    
    if (nextModification) {
      // console.log('[NEXT] Moving to next modification');
      nextModification.click();
      currentSelection.modification = nextModification;
      
      const updatedRubbings = await waitForListUpdate(persisted.rubbings, 'rubbing', 10000, true);
      persisted.rubbings = updatedRubbings;
      await selectFirstItem('rubbing');
      return;
    }
    
    // console.log('[LOGIC] No next modification. Checking suspension...');
    const currentSuspensionItem = findCurrentSelectedItem('suspension');
    const nextSuspension = getNextUniqueItem(currentSuspensionItem, 'suspension');
    
    if (nextSuspension) {
      // console.log('[NEXT] Moving to next suspension');
      nextSuspension.click();
      currentSelection.suspension = nextSuspension;
      
      const updatedModifications = await waitForListUpdate(persisted.modifications, 'modification', 10000, true);
      persisted.modifications = updatedModifications;
      await selectFirstItem('modification');
      
      const updatedRubbings = await waitForListUpdate(persisted.rubbings, 'rubbing', 10000, true);
      persisted.rubbings = updatedRubbings;
      await selectFirstItem('rubbing');
      return;
    }
    
    // console.log('[LOGIC] No next suspension. Checking drive...');
    const currentDriveItem = findCurrentSelectedItem('drive');
    const nextDrive = getNextUniqueItem(currentDriveItem, 'drive');
    
    if (nextDrive) {
      // console.log('[NEXT] Moving to next drive');
      nextDrive.click();
      currentSelection.drive = nextDrive;
      
      const updatedSuspensions = await waitForListUpdate(persisted.suspensions, 'suspension', 10000, true);
      persisted.suspensions = updatedSuspensions;
      await selectFirstItem('suspension');
      
      const updatedModifications = await waitForListUpdate(persisted.modifications, 'modification', 10000, true);
      persisted.modifications = updatedModifications;
      await selectFirstItem('modification');
      
      const updatedRubbings = await waitForListUpdate(persisted.rubbings, 'rubbing', 10000, true);
      persisted.rubbings = updatedRubbings;
      await selectFirstItem('rubbing');
      return;
    }
    
    // console.log('[LOGIC] No next drive. Checking trim...');
  } else {
    const currentDriveItem = findCurrentSelectedItem('drive');
    const nextDrive = getNextUniqueItem(currentDriveItem, 'drive');
    
    if (nextDrive) {
      // console.log('[NEXT] Moving to next drive');
      nextDrive.click();
      currentSelection.drive = nextDrive;
      return;
    }
    
    // console.log('[LOGIC] No next drive. Checking trim...');
  }
  
  const currentTrimItem = findCurrentSelectedItem('trim');
  const nextTrim = getNextUniqueItem(currentTrimItem, 'trim');
  
  if (nextTrim) {
    // console.log('[NEXT] Moving to next trim');
    nextTrim.click();
    currentSelection.trim = nextTrim;
    
    const updatedDrives = await waitForListUpdate(persisted.drives, 'drive');
    persisted.drives = updatedDrives;
    await selectFirstItem('drive');
    
    if (handleExtraDropdowns) {
      const updatedSuspensions = await waitForListUpdate(persisted.suspensions, 'suspension', 10000, true);
      persisted.suspensions = updatedSuspensions;
      await selectFirstItem('suspension');
      
      const updatedModifications = await waitForListUpdate(persisted.modifications, 'modification', 10000, true);
      persisted.modifications = updatedModifications;
      await selectFirstItem('modification');
      
      const updatedRubbings = await waitForListUpdate(persisted.rubbings, 'rubbing', 10000, true);
      persisted.rubbings = updatedRubbings;
      await selectFirstItem('rubbing');
    }
    return;
  }
  
  // console.log('[LOGIC] No next trim. Checking model...');
  const currentModelItem = findCurrentSelectedItem('model');
  const nextModel = getNextUniqueItem(currentModelItem, 'model');
  
  if (nextModel) {
    // console.log('[NEXT] Moving to next model');
    nextModel.click();
    currentSelection.model = nextModel;
    
    const updatedTrims = await waitForListUpdate(persisted.trims, 'trim');
    persisted.trims = updatedTrims;
    await selectFirstItem('trim');
    
    const updatedDrives = await waitForListUpdate(persisted.drives, 'drive');
    persisted.drives = updatedDrives;
    await selectFirstItem('drive');
    
    if (handleExtraDropdowns) {
      const updatedSuspensions = await waitForListUpdate(persisted.suspensions, 'suspension', 10000, true);
      persisted.suspensions = updatedSuspensions;
      await selectFirstItem('suspension');
      
      const updatedModifications = await waitForListUpdate(persisted.modifications, 'modification', 10000, true);
      persisted.modifications = updatedModifications;
      await selectFirstItem('modification');
      
      const updatedRubbings = await waitForListUpdate(persisted.rubbings, 'rubbing', 10000, true);
      persisted.rubbings = updatedRubbings;
      await selectFirstItem('rubbing');
    }
    return;
  }
  
  // console.log('[LOGIC] No next model. Checking make...');
  const currentMakeItem = findCurrentSelectedItem('make');
  const nextMake = getNextUniqueItem(currentMakeItem, 'make');
  
  if (nextMake) {
    // console.log('[NEXT] Moving to next make');
    nextMake.click();
    currentSelection.make = nextMake;
    
    const updatedModels = await waitForListUpdate(persisted.models, 'model');
    persisted.models = updatedModels;
    await selectFirstItem('model');
    
    const updatedTrims = await waitForListUpdate(persisted.trims, 'trim');
    persisted.trims = updatedTrims;
    await selectFirstItem('trim');
    
    const updatedDrives = await waitForListUpdate(persisted.drives, 'drive');
    persisted.drives = updatedDrives;
    await selectFirstItem('drive');
    
    if (handleExtraDropdowns) {
      const updatedSuspensions = await waitForListUpdate(persisted.suspensions, 'suspension', 10000, true);
      persisted.suspensions = updatedSuspensions;
      await selectFirstItem('suspension');
      
      const updatedModifications = await waitForListUpdate(persisted.modifications, 'modification', 10000, true);
      persisted.modifications = updatedModifications;
      await selectFirstItem('modification');
      
      const updatedRubbings = await waitForListUpdate(persisted.rubbings, 'rubbing', 10000, true);
      persisted.rubbings = updatedRubbings;
      await selectFirstItem('rubbing');
    }
    return;
  }
  
  // console.log('[LOGIC] No next make. Checking year...');
  const currentYearItem = findCurrentSelectedItem('year');
  const nextYear = getNextUniqueItem(currentYearItem, 'year');
  
  if (nextYear) {
    // Check for year limit before proceeding
    if (yearLimit) {
      const nextYearValue = parseInt(nextYear.textContent.trim());
      const limitValue = parseInt(yearLimit);
      
      // console.log(`[LOGIC] Checking year limit. Next year is ${nextYearValue}, limit is ${limitValue}.`);
      
      if (nextYearValue < limitValue) {
        console.log('[STOP] Year limit is reached.');
        throw new Error('STOP');
      }
    }
    
    // console.log('[NEXT] Moving to next year');
    nextYear.click();
    currentSelection.year = nextYear;
    
    const updatedMakes = await waitForListUpdate(persisted.makes, 'make');
    persisted.makes = updatedMakes;
    await selectFirstItem('make');
    
    const updatedModels = await waitForListUpdate(persisted.models, 'model');
    persisted.models = updatedModels;
    await selectFirstItem('model');
    
    const updatedTrims = await waitForListUpdate(persisted.trims, 'trim');
    persisted.trims = updatedTrims;
    await selectFirstItem('trim');
    
    const updatedDrives = await waitForListUpdate(persisted.drives, 'drive');
    persisted.drives = updatedDrives;
    await selectFirstItem('drive');
    
    if (handleExtraDropdowns) {
      const updatedSuspensions = await waitForListUpdate(persisted.suspensions, 'suspension', 10000, true);
      persisted.suspensions = updatedSuspensions;
      await selectFirstItem('suspension');
      
      const updatedModifications = await waitForListUpdate(persisted.modifications, 'modification', 10000, true);
      persisted.modifications = updatedModifications;
      await selectFirstItem('modification');
      
      const updatedRubbings = await waitForListUpdate(persisted.rubbings, 'rubbing', 10000, true);
      persisted.rubbings = updatedRubbings;
      await selectFirstItem('rubbing');
    }
    return;
  }
  
  // console.log('[LOGIC] No next year found.');
  console.log('[COMPLETE] No more data found.');
  throw new Error('COMPLETE');
}

console.log("Navigation code inserted completed successfully");

// ───────────────────────────────────────────────────────────────────
// USAGE EXAMPLES
// ───────────────────────────────────────────────────────────────────

// Example 1: Start from year 2025, with 8 dropdowns, and a minimum year of 2024.
// This will select the 2025 combination first, and the next one will be 2024.
// It will stop when it tries to go below 2024.
// navigateAndApplyNext({}, true, "2025", "2024");

// Example 2: Resume from a specific combination. startYear is ignored.
// navigateAndApplyNext({
//   year: "2023",
//   make: "Acura",
//   model: "Integra"
// }, true, "2025", "2024"); // "2025" will be ignored here.

// Example 3: Start from the first year that is >= 2024.
// navigateAndApplyNext({}, true, null, "2024");