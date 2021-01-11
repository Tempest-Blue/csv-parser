const fs = require('fs').promises;

async function readFile(filePath) {
    try {
        return fs.readFile(filePath, 'utf8');
    } catch (err) {
        return 'Cannot read file ' + filePath + ' - ' + err;
    }
}

function log(err) {
    return 'Error - ' + err;
}

(async () => {
    try {
        // Read new database
        var newFile = await readFile('new.csv');
        // Check if new database blank
        if (!newFile.length) {
            return log('New database blank.');
        }

        // Read old database
        var oldFile = await readFile('old.csv');
        // If only old db blank, we don't need to do any processing.
        // New db file has to have something to get to this point.
        if (!oldFile.length) {
            return log('Old database blank so all records in new database are newly created.');
        }

        // Both files read successfully
        // Store bigger db as object based on primary key
        newFile = newFile.split('\n');
        oldFile = oldFile.split('\n');

        var oldDB = {};
        var newDB = {};
        var missing = {};
        var corrupted = {};
        var newlyCreated = {};
        var matched = 0;
        // Create object from smaller database
        for (line in oldFile) {
            // Line must not be blank
            if (oldFile[line].length) {
                var currentLine = oldFile[line].split(',');
                var primaryKey = currentLine[0];
                currentLine.shift();
                oldDB[primaryKey] = currentLine;
            }
        }

        missing = JSON.parse(JSON.stringify(oldDB));
        // Reference old db object as we're going through new db
        for (line in newFile) {
            // Line must not be blank
            if (newFile[line].length) {
                var currentLine = newFile[line].split(',');
                var primaryKey = currentLine[0];
                currentLine.shift();
                newDB[primaryKey] = currentLine;

                // Newly created if not found in old DB.
                if (oldDB[primaryKey] == null) {
                    newlyCreated[primaryKey] = currentLine;
                }    
                else {
                    // Corrupted if not all old db columns match.
                    var oldDbLineArray = oldDB[primaryKey];
                    var corruptedLine = false;
                    for (line in oldDbLineArray) {
                        if (oldDbLineArray[line] != currentLine[line]) {
                            corrupted[primaryKey] = currentLine;
                            corruptedLine = true;
                        }
                    }
                    if (!corruptedLine) {
                        matched++;
                    }
                    // Delete record. We want to be left with the missed records
                    delete missing[primaryKey];
                }
            }
        }

        // Logging
        console.log('Old DB Size - ' +  (oldFile.length - 1));
        console.log('New DB Size - ' +  (newFile.length - 1));
        console.log('Lines Matching - ' +  matched);
        console.log('Lines Missing - ' +  Object.keys(missing).length);
        console.log('Lines Corrupted - ' + Object.keys(corrupted).length);
        console.log('Newly Created - ' + Object.keys(newlyCreated).length + '\n');

        // Testing
        var missingArray = Object.keys(missing);
        if (missingArray.length) {
            var randomMissingIndex = Math.floor(Math.random() * (missingArray.length + 1)); // random number between 0 and the size of missing
            var missingTestPrimaryKey = missingArray[randomMissingIndex];
            console.log('Missing Test Passed - ' + (oldDB[missingTestPrimaryKey] && !newDB[missingTestPrimaryKey]));
        }
        else {
            console.log('Missing Test Passed - No records missing');
        }

        var newlyCreatedArray = Object.keys(newlyCreated);
        if (newlyCreatedArray.length) {
            var randomNewlyCreatedIndex = Math.floor(Math.random() * (newlyCreatedArray.length + 1)); // random number between 0 and the size of newlyCreated
            var newlyCreatedTestPrimaryKey = newlyCreatedArray[randomNewlyCreatedIndex];
            console.log('Newly Created Test Passed - ' + (!oldDB[newlyCreatedTestPrimaryKey] && (newDB[newlyCreatedTestPrimaryKey] != null)));
        }
        else {
            console.log('Newly Created Test Passed - No newly created records');
        }

        var corruptedArray = Object.keys(corrupted);
        if (corruptedArray.length) {
            var randomCorruptedIndex = Math.floor(Math.random() * (corruptedArray.length + 1)); // random number between 0 and the size of corrupted
            var corruptedTestPrimaryKey = corruptedArray[randomCorruptedIndex];
            var oldDbCorruptionArray = oldDB[corruptedTestPrimaryKey];
            var newDbCorruptionArray = newDB[corruptedTestPrimaryKey];
            var corruptionTestPassed = false;
            for (index in oldDbCorruptionArray) {
                if (oldDbCorruptionArray[index] != newDbCorruptionArray) {
                    console.log('Corrupted Test Passed - ' + 'Old[' + oldDB[corruptedTestPrimaryKey] + '], New[' + newDB[corruptedTestPrimaryKey] + ']');
                    corruptionTestPassed = true;
                    break;
                }
            }
            if (!corruptionTestPassed) {
                console.log('Corrupted Test Failed');
            }
        }
        else {
            console.log('Corrupted Test Passed - No records corrupted');
        }

        // Identified Data 
        console.log('Missing - ' + JSON.stringify(missing) + '\n\n\n');
        console.log('Corrupted - ' + JSON.stringify(corrupted) + '\n\n\n');
        console.log('Created - ' + JSON.stringify(newlyCreated) + '\n\n\n');
        
    } catch (err) {
        return log(err);
    }
})();