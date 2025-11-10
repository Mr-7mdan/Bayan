# Datasource Ownership Transfer Wizard

An interactive command-line wizard to transfer datasource ownership between users in Bayan.

## Features

- 🎨 **Beautiful interactive UI** with color-coded output
- 🔍 **Auto-detects database location** - no configuration needed
- ✅ **Safe operations** with confirmation prompts
- 📊 **Clear summaries** showing exactly what will be transferred
- 🔄 **Batch transfers** - select multiple datasources at once
- 🛡️ **Handles shares** - automatically removes conflicting shares

## Usage

### From Project Root

```bash
python scripts/transfer_datasources.py
```

### From Scripts Directory

```bash
cd scripts
python transfer_datasources.py
```

### Direct Execution (Unix/Mac)

```bash
./scripts/transfer_datasources.py
```

## How It Works

The wizard guides you through 5 simple steps:

### Step 1: Select Source User
- Lists all active users in the system
- Select the current owner of the datasources

### Step 2: Select Datasources
- Shows all datasources owned by the source user
- Select one or more datasources (or type 'all')
- Displays datasource details: name, type, creation date

### Step 3: Select Target User
- Lists all users except the source user
- Select the new owner for the datasources

### Step 4: Confirm Transfer
- Shows a detailed summary of what will be transferred
- Lists any existing shares that will be affected
- Requires explicit confirmation

### Step 5: Execute Transfer
- Updates datasource ownership in the database
- Removes conflicting shares (if target user already had a share)
- Preserves all other shares

## Example Session

```
======================================================================
            DATASOURCE OWNERSHIP TRANSFER WIZARD
======================================================================

✓ Found database at: /path/to/.data/meta.sqlite

======================================================================
              Step 1: Select Source User (Current Owner)
======================================================================

#    Name                           Email                                    Role      
---- ------------------------------ ---------------------------------------- ----------
1    John Doe                       john@example.com                         admin     
2    Jane Smith                     jane@example.com                         user      
3    Bob Wilson                     bob@example.com                          user      

Select source user:
> 1

✓ Selected source user: John Doe (john@example.com)

======================================================================
           Step 2: Select Datasources to Transfer
======================================================================

#    Name                                     Type            Created             
---- ---------------------------------------- --------------- --------------------
1    Sales Database                           postgres        2024-01-15 10:30:00 
2    Marketing Analytics                      mysql           2024-02-20 14:45:00 
3    Customer Data                            csv             2024-03-10 09:15:00 

ℹ Total: 3 datasource(s)

Select datasources to transfer (enter numbers separated by commas, or 'all'):
> 1,3

✓ Selected 2 datasource(s) for transfer

======================================================================
              Step 3: Select Target User (New Owner)
======================================================================

#    Name                           Email                                    Role      
---- ------------------------------ ---------------------------------------- ----------
1    Jane Smith                     jane@example.com                         user      
2    Bob Wilson                     bob@example.com                          user      

Select target user:
> 1

✓ Selected target user: Jane Smith (jane@example.com)

======================================================================
                    Step 4: Confirm Transfer
======================================================================

Transfer Summary:
  From: John Doe (john@example.com)
  To:   Jane Smith (jane@example.com)

Datasources to transfer:
  • Sales Database (postgres)
  • Customer Data (csv)

⚠ This action will:
  1. Change ownership of 2 datasource(s)
  2. Remove any existing shares the target user has for these datasources
  3. Preserve all other shares

⚠ Do you want to proceed with this transfer? (yes/no): yes

======================================================================
              Step 5: Transferring Datasources
======================================================================

✓ Successfully transferred 2 datasource(s) to Jane Smith

======================================================================
                    Transfer Complete!
======================================================================

✓ All 2 datasource(s) have been transferred successfully
ℹ Jane Smith is now the owner of the selected datasources
```

## What Gets Changed

### Database Tables Affected

1. **`datasources` table**
   - Updates `user_id` field to the new owner

2. **`datasource_shares` table**
   - Removes any existing share entry if the target user already had access
   - (This prevents duplicate ownership - you can't own AND share to yourself)
   - All other shares remain intact

### What Is NOT Changed

- The actual data in the datasources
- Datasource configuration or connection settings
- Shares to other users
- Dashboards or widgets using these datasources
- Historical data or audit logs

## Safety Features

- ✅ Shows confirmation prompt before making changes
- ✅ Displays detailed summary of what will change
- ✅ Validates all user input
- ✅ Uses database transactions (rollback on error)
- ✅ Only affects selected datasources
- ✅ Preserves database integrity

## Error Handling

The wizard handles common scenarios:

- **Database not found**: Checks multiple common locations
- **No users**: Exits gracefully if database is empty
- **No datasources**: Notifies if selected user has no datasources
- **Invalid input**: Re-prompts for valid selection
- **Ctrl+C**: Cancels operation cleanly
- **Database errors**: Rolls back changes and reports error

## Requirements

- Python 3.7+
- Access to the Bayan metadata database (`meta.sqlite`)
- No additional dependencies (uses only Python standard library)

## Notes

- The script must be run on the same machine where the database is located
- All users and datasources must be active (not soft-deleted)
- The script uses SQLite transactions for data consistency
- Database backups are recommended before bulk transfers

## Troubleshooting

### Database Not Found

If the script can't find the database, run it from:
- The project root directory, OR
- The backend directory, OR  
- Specify the path manually by editing the `find_database()` function

### Permission Denied

On Unix/Mac, make sure the script is executable:
```bash
chmod +x scripts/transfer_datasources.py
```

### No Users Found

Ensure the backend has been initialized and at least one user has been created.

## Support

For issues or questions, check the Bayan documentation or contact your system administrator.
