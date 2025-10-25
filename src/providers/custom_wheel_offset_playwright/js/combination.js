// after fixing missing vehicle info
// Helper function to wait for a specified time
async function wait(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Helper function to get browser cookies
function getBrowserCookies() {
    return document.cookie;
}

// Helper function to parse HTML and extract select options
function parseSelectOptions(html, selectId) {
    const tempDiv = document.createElement('div');
    tempDiv.innerHTML = html;
    const selectElement = tempDiv.querySelector(`#${selectId}`);
    if (!selectElement) {
        return [];
    }
    return Array.from(selectElement.options).map(option => ({
        value: option.value,
        text: option.textContent.trim(),
        selected: option.selected
    }));
}

// Function to make an API call with proper headers and retry logic
async function makeApiCall(url, headers = {}, maxRetries = 20) {
    const cookies = getBrowserCookies();
    const defaultHeaders = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "sec-ch-ua": "\"Google Chrome\";v=\"141\", \"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"141\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "cookie": cookies,
        ...headers
    };
    
    let retryCount = 0;
    while (retryCount < maxRetries) {
        try {
            const response = await fetch(url, { headers: defaultHeaders });
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return await response.text();
        } catch (error) {
            retryCount++;
            if (retryCount >= maxRetries) {
                throw new Error(`Failed after ${maxRetries} attempts`);
            }
            await wait(1000);
        }
    }
}

// Function to make a JSON API call with proper headers and retry logic
async function makeJsonApiCall(url, headers = {}, maxRetries = 20) {
    const cookies = getBrowserCookies();
    const defaultHeaders = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "sec-ch-ua": "\"Google Chrome\";v=\"141\", \"Not?A_Brand\";v=\"8\", \"Chromium\";v=\"141\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "cookie": cookies,
        ...headers
    };
    
    let retryCount = 0;
    while (retryCount < maxRetries) {
        try {
            const response = await fetch(url, { headers: defaultHeaders });
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return await response.json();
        } catch (error) {
            retryCount++;
            if (retryCount >= maxRetries) {
                throw new Error(`Failed after ${maxRetries} attempts`);
            }
            await wait(1000);
        }
    }
}

// Function to get vehicle information
async function getVehicleInfo(year, make, model, trim, drive) {
    const url = `https://www.enthusiastenterprises.us/fitment/vehicle/co/${year}/${make}/${model}/${trim}/${drive}`;
    // Remove the 'priority' header for this specific domain to avoid CORS issues
    const headers = {
        "sec-fetch-site": "cross-site",
        "Referer": "https://www.customwheeloffset.com/"
    };
    
    try {
        const data = await makeJsonApiCall(url, headers);
        
        // Check if the response is empty or missing required fields
        if (!data || !data.drchassisid || !data.vehicleType || !data.boltpattern) {
            return null;
        }
        
        return {
            drchassisid: data.drchassisid,
            vehicleType: data.vehicleType,
            boltPattern: data.boltpattern
        };
    } catch (error) {
        // Silently handle errors
        return null;
    }
}

// Function to set vehicle context (for bot detection)
async function setVehicleContext(vehicleType, year, make, model, trim, drive, chassisId) {
    const url = `https://www.customwheeloffset.com/api/ymm-temp.php?store=wheels&type=set&vehicle_type=${vehicleType}&year=${year}&make=${make}&model=${model}&trim=${trim}&drive=${drive}&chassis=${chassisId}`;
    const headers = {
        "sec-fetch-site": "same-origin",
        "x-requested-with": "XMLHttpRequest",
        "Referer": "https://www.customwheeloffset.com/store/wheels"
    };
    
    try {
        await makeApiCall(url, headers);
        return true;
    } catch (error) {
        console.error(`[ERROR] Failed to set vehicle context:`, error.message);
        return false;
    }
}

// Function to get suspension options
async function getSuspensionOptions(vehicleType) {
    const url = `https://www.customwheeloffset.com/api/ymm-temp.php?type=set&vehicle_type=${vehicleType}&store=wheels&getSuspension=true`;
    const headers = {
        "sec-fetch-site": "same-origin",
        "x-requested-with": "XMLHttpRequest",
        "Referer": "https://www.customwheeloffset.com/store/wheels"
    };
    
    try {
        const data = await makeJsonApiCall(url, headers);
        return data;
    } catch (error) {
        console.error(`[ERROR] Failed to get suspension options:`, error.message);
        return [];
    }
}

// Function to get modification options
async function getModificationOptions(vehicleType) {
    const url = `https://www.customwheeloffset.com/api/ymm-temp.php?type=set&vehicle_type=${vehicleType}&store=wheels&getTrimming=true`;
    const headers = {
        "sec-fetch-site": "same-origin",
        "x-requested-with": "XMLHttpRequest",
        "Referer": "https://www.customwheeloffset.com/store/wheels"
    };
    
    try {
        const data = await makeJsonApiCall(url, headers);
        return data;
    } catch (error) {
        console.error(`[ERROR] Failed to get modification options:`, error.message);
        return [];
    }
}

// Function to get rubbing options
async function getRubbingOptions(vehicleType) {
    const url = `https://www.customwheeloffset.com/api/ymm-temp.php?type=set&vehicle_type=${vehicleType}&store=wheels&getRubbing=true`;
    const headers = {
        "sec-fetch-site": "same-origin",
        "x-requested-with": "XMLHttpRequest",
        "Referer": "https://www.customwheeloffset.com/store/wheels"
    };
    
    try {
        const data = await makeJsonApiCall(url, headers);
        return data;
    } catch (error) {
        console.error(`[ERROR] Failed to get rubbing options:`, error.message);
        return [];
    }
}

// Function to create all combinations of options
function createCombinations(vehicleInfo, suspensionOptions, modificationOptions, rubbingOptions, vehicleData) {
    const combinations = [];
    
    for (const suspension of suspensionOptions) {
        for (const modification of modificationOptions) {
            for (const rubbing of rubbingOptions) {
                combinations.push({
                    ...vehicleInfo,
                    vehicle_type: vehicleData.vehicleType,
                    dr_chassis_id: vehicleData.drchassisid,
                    bolt_pattern: vehicleData.boltPattern,
                    suspension,
                    modification,
                    rubbing
                });
            }
        }
    }
    
    return combinations;
}

// Main function to navigate through the dropdowns with optional start and end points
async function startScraping(options = {}, includeOptions = true, limit = 0) {
    const { starts, ends } = options;
    
    // Counter for the number of valid year-make-model-trim-drive combinations processed
    let validCombinationCount = 0;
    
    try {
        // Step 1: Get years
        const yearsHtml = await makeApiCall('https://www.customwheeloffset.com/makemodel/bp.php');
        let yearOptions = parseSelectOptions(yearsHtml, 'year').filter(opt => opt.value !== '');
        
        // Filter years based on the start year
        if (starts && starts.year) {
            const startIdx = yearOptions.findIndex(opt => opt.value === starts.year);
            if (startIdx !== -1) {
                yearOptions = yearOptions.slice(startIdx);
            }
        }
        
        // Loop through years
        for (const yearOption of yearOptions) {
            // Check if we've reached the end point at the year level
            if (ends && ends.year && yearOption.value === ends.year && !ends.make) {
                return;
            }

            const makesHtml = await makeApiCall(`https://www.customwheeloffset.com/makemodel/bp.php?year=${yearOption.value}`);
            let makeOptions = parseSelectOptions(makesHtml, 'make').filter(opt => opt.value !== '');
            
            // Filter makes based on the start make (only for the start year)
            if (starts && starts.year && yearOption.value === starts.year && starts.make) {
                const startIdx = makeOptions.findIndex(opt => opt.value === starts.make);
                if (startIdx !== -1) {
                    makeOptions = makeOptions.slice(startIdx);
                }
            }

            // Loop through makes
            for (const makeOption of makeOptions) {
                // Check if we've reached the end point at the make level
                if (ends && ends.year && yearOption.value === ends.year && ends.make && makeOption.value === ends.make && !ends.model) {
                    return;
                }

                const modelsHtml = await makeApiCall(`https://www.customwheeloffset.com/makemodel/bp.php?year=${yearOption.value}&make=${makeOption.value}`);
                let modelOptions = parseSelectOptions(modelsHtml, 'model').filter(opt => opt.value !== '');

                // Filter models based on the start model (only for the start year and make)
                if (starts && starts.year && yearOption.value === starts.year && starts.make && makeOption.value === starts.make && starts.model) {
                    const startIdx = modelOptions.findIndex(opt => opt.value === starts.model);
                    if (startIdx !== -1) {
                        modelOptions = modelOptions.slice(startIdx);
                    }
                }

                // Loop through models
                for (const modelOption of modelOptions) {
                    // Check if we've reached the end point at the model level
                    if (ends && ends.year && yearOption.value === ends.year && ends.make && makeOption.value === ends.make && ends.model && modelOption.value === ends.model && !ends.trim) {
                        return;
                    }

                    const trimsHtml = await makeApiCall(`https://www.customwheeloffset.com/makemodel/bp.php?year=${yearOption.value}&make=${makeOption.value}&model=${modelOption.value}`);
                    let trimOptions = parseSelectOptions(trimsHtml, 'trim').filter(opt => opt.value !== '');

                    // Filter trims based on the start trim (only for the start year, make, and model)
                    if (starts && starts.year && yearOption.value === starts.year && starts.make && makeOption.value === starts.make && starts.model && modelOption.value === starts.model && starts.trim) {
                        const startIdx = trimOptions.findIndex(opt => opt.value === starts.trim);
                        if (startIdx !== -1) {
                            trimOptions = trimOptions.slice(startIdx);
                        }
                    }

                    // Loop through trims
                    for (const trimOption of trimOptions) {
                        // Check if we've reached the end point at the trim level
                        if (ends && ends.year && yearOption.value === ends.year && ends.make && makeOption.value === ends.make && ends.model && modelOption.value === ends.model && ends.trim && trimOption.value === ends.trim && !ends.drive) {
                            return;
                        }

                        const drivesHtml = await makeApiCall(`https://www.customwheeloffset.com/makemodel/bp.php?year=${yearOption.value}&make=${makeOption.value}&model=${modelOption.value}&trim=${trimOption.value}`);
                        let driveOptions = parseSelectOptions(drivesHtml, 'drive').filter(opt => opt.value !== '');

                        // Filter drives based on the start drive (only for the start year, make, model, and trim)
                        if (starts && starts.year && yearOption.value === starts.year && starts.make && makeOption.value === starts.make && starts.model && modelOption.value === starts.model && starts.trim && trimOption.value === starts.trim && starts.drive) {
                            const startIdx = driveOptions.findIndex(opt => opt.value === starts.drive);
                            if (startIdx !== -1) {
                                // Skip the exact drive and start with the next one
                                driveOptions = driveOptions.slice(startIdx + 1);
                            }
                        }

                        // Loop through drives
                        for (const driveOption of driveOptions) {
                            // Check if this is the exact start combination, and if so, skip it.
                            if (starts && starts.year === yearOption.value && starts.make === makeOption.value && starts.model === modelOption.value && starts.trim === trimOption.value && starts.drive === driveOption.value) {
                                continue;
                            }

                            // Check if we've reached the end point at the drive level
                            if (ends && ends.year && yearOption.value === ends.year && ends.make && makeOption.value === ends.make && ends.model && modelOption.value === ends.model && ends.trim && trimOption.value === ends.trim && ends.drive && driveOption.value === ends.drive) {
                                return;
                            }

                            // Create the base vehicle info object
                            const vehicleInfo = {
                                year: yearOption.text,
                                make: makeOption.text,
                                model: modelOption.text,
                                trim: trimOption.text,
                                drive: driveOption.text
                            };

                            // Get vehicle information
                            const vehicleData = await getVehicleInfo(
                                yearOption.value, 
                                makeOption.value, 
                                modelOption.value, 
                                trimOption.value, 
                                driveOption.value
                            );

                            // Skip if vehicle data is missing
                            if (!vehicleData) {
                                continue;
                            }

                            // Increment the valid combination counter
                            validCombinationCount++;
                            
                            // Check if we've reached the limit
                            if (limit > 0 && validCombinationCount > limit) {
                                console.log(`[LIMIT] Reached limit of ${limit} valid year-make-model-trim-drive combinations`);
                                return;
                            }

                            if (includeOptions) {
                                try {
                                    // Set vehicle context (for bot detection)
                                    await setVehicleContext(
                                        vehicleData.vehicleType,
                                        yearOption.value,
                                        makeOption.value,
                                        modelOption.value,
                                        trimOption.value,
                                        driveOption.value,
                                        vehicleData.drchassisid
                                    );

                                    // Get all options
                                    const suspensionOptions = await getSuspensionOptions(vehicleData.vehicleType);
                                    const modificationOptions = await getModificationOptions(vehicleData.vehicleType);
                                    const rubbingOptions = await getRubbingOptions(vehicleData.vehicleType);

                                    // Create all combinations
                                    const combinations = createCombinations(
                                        vehicleInfo,
                                        suspensionOptions,
                                        modificationOptions,
                                        rubbingOptions,
                                        vehicleData
                                    );

                                    // Log each combination as a string
                                    for (const combination of combinations) {
                                        console.log(JSON.stringify(combination, null, 2));
                                    }
                                } catch (error) {
                                    console.error(`[ERROR] Failed to process vehicle ${vehicleInfo.year} ${vehicleInfo.make} ${vehicleInfo.model} ${vehicleInfo.trim} ${vehicleInfo.drive}:`, error.message);
                                    continue;
                                }
                            } else {
                                try {
                                    // Add the additional fields to the vehicle info
                                    const enhancedVehicleInfo = {
                                        ...vehicleInfo,
                                        vehicle_type: vehicleData.vehicleType,
                                        dr_chassis_id: vehicleData.drchassisid,
                                        bolt_pattern: vehicleData.boltPattern
                                    };

                                    // Log the enhanced vehicle info as a string
                                    console.log(JSON.stringify(enhancedVehicleInfo, null, 2));
                                } catch (error) {
                                    console.error(`[ERROR] Failed to process vehicle ${vehicleInfo.year} ${vehicleInfo.make} ${vehicleInfo.model} ${vehicleInfo.trim} ${vehicleInfo.drive}:`, error.message);
                                    continue;
                                }
                            }
                            
                            await wait(500);
                        }
                    }
                }
            }
        }
        
        // If we've reached here, it means all data has been processed
        console.log("No more data left");
    } catch (error) {
        console.error("[FATAL] Error in dropdown navigation:", error);
    }
}
console.log("combination scraping script loaded successfully")
// Example usage:
// To scrape all combinations with options: startScraping();
// To scrape from a specific start point with options: startScraping({starts: {year: "2026", make: "Acura", model:"Integra", trim: "Base", drive: "AWD"}});
// To scrape from a start point to an end point with options: startScraping({starts: {year: "2026", make: "Acura", model:"Integra", trim: "Base", drive: "AWD"}, ends: {year: "2020", make: "Acura", model:"Integra", trim: "Base", drive: "AWD"}});
// To scrape all combinations without options: startScraping({}, false);
// To scrape from a specific start point without options: startScraping({starts: {year: "2026", make: "Acura", model:"Integra", trim: "Base", drive: "AWD"}}, false);
// To scrape with a limit of 2 year-make-model-trim-drive combinations with options: startScraping({}, true, 2);
// To scrape with a limit of 5 year-make-model-trim-drive combinations without options: startScraping({starts: {year: "2026", make: "Acura", model:"Integra", trim: "Base", drive: "AWD"}}, false, 5);