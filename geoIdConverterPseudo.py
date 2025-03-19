# For each geoId
    # BEGIN TRANSACTION
        # Instantiate array for splitNumbers
        # If a place with this geoId already exists, continue
        # If geoId.len == 2
            # Fips state code
        # If geoId.len == 4
            # # (2+2=4)
            # First 2 is the state
            # Next 2 is the congressional district
            # Push each int to splitNumbers
        # If geoId.len == 5
            # # (2+3=5)
            # First 2 are the state
            # Next 3 are the county or the State Legislative District
            # Push each int to splitNumbers
        # If geoId.len == 7
            # (2+5=7)
            # First 2 is the state
            # Next 5 is the Place/City/School District
            # Push each int to splitNumbers
        # if geoId.len == 10
            # # (2+3+5=10)
            # First 2 are the state
            # Next 3 are the county
            # Next 5 is the county subdivision or place
            # Push each int to splitNumbers
        # If geoId.len == 11
            # # (2+3+6=11)
            # First 2 is the state
            # Next 3 is the county
            # Next 6 is the census tract
            # Push each int to splitNumbers
        # if geoId.len == 12
            # # (2+3+6+1=12)
            # First 2 is the state
            # Next 3 is the county
            # Next 6 is the Tract
            # Next 1 is the Block Group number
            # Push each int to splitNumbers
        # Call splitGeoId
        # Check against master list of geoId's, take the name of the entity
        # If it doesn't exist, create a Place with that geoId and name, and other Place data from BR
        # Take returned Place Id and set it as the parent of the Place created on the previous line
    # If any operation fails
        # ROLLBACK TRANSACTION
    # Else
        # COMMIT TRANSACTION

# func splitGeoId(int[] splitIndex, string geoId)
    # for each splitIndex
        # Subtract by one for zero-index
        # split the geoId from index 0 (or from previous index split) up to splitIndex
        # Check that FIPS code against master list of geoIds
        # If a Place for that geoId doesn't exist, create it, and populate it with BR data
        # If index of loop is > 1, assign previously found/created Place as the currently found/created Place's parent
    # Return ID of last created/found Place