#!/usr/bin/env python3
"""
Interactive wizard to transfer ownership between users.
Supports datasources and dashboards.
Run this script on the host machine with local backend access.
"""

import sqlite3
import sys
import os
from pathlib import Path
from typing import List, Tuple, Optional
from datetime import datetime


# Enable ANSI colors on Windows
def enable_windows_ansi():
    """Enable ANSI escape sequences on Windows 10+"""
    if sys.platform == "win32":
        try:
            import ctypes
            kernel32 = ctypes.windll.kernel32
            # Enable ANSI escape sequences in Windows console
            kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
        except Exception:
            # If enabling fails, disable colors
            return False
    return True


# Check if colors should be enabled
COLORS_ENABLED = enable_windows_ansi()


class Colors:
    """ANSI color codes for terminal output"""
    if COLORS_ENABLED:
        HEADER = '\033[95m'
        OKBLUE = '\033[94m'
        OKCYAN = '\033[96m'
        OKGREEN = '\033[92m'
        WARNING = '\033[93m'
        FAIL = '\033[91m'
        ENDC = '\033[0m'
        BOLD = '\033[1m'
        UNDERLINE = '\033[4m'
    else:
        # Disable colors if ANSI not supported
        HEADER = ''
        OKBLUE = ''
        OKCYAN = ''
        OKGREEN = ''
        WARNING = ''
        FAIL = ''
        ENDC = ''
        BOLD = ''
        UNDERLINE = ''


def print_header(text: str):
    """Print a styled header"""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*70}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text.center(70)}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'='*70}{Colors.ENDC}\n")


def print_success(text: str):
    """Print success message"""
    print(f"{Colors.OKGREEN}✓ {text}{Colors.ENDC}")


def print_error(text: str):
    """Print error message"""
    print(f"{Colors.FAIL}✗ {text}{Colors.ENDC}")


def print_warning(text: str):
    """Print warning message"""
    print(f"{Colors.WARNING}⚠ {text}{Colors.ENDC}")


def print_info(text: str):
    """Print info message"""
    print(f"{Colors.OKCYAN}ℹ {text}{Colors.ENDC}")


def find_database() -> Optional[Path]:
    """Find the metadata database file"""
    # Check common locations
    possible_paths = [
        Path.cwd() / ".data" / "meta.sqlite",
        Path.cwd() / "backend" / ".data" / "meta.sqlite",
        Path(__file__).parent.parent / ".data" / "meta.sqlite",
        Path(__file__).parent.parent / "backend" / ".data" / "meta.sqlite",
    ]
    
    for path in possible_paths:
        if path.exists():
            return path
    
    return None


def get_connection() -> sqlite3.Connection:
    """Get database connection"""
    db_path = find_database()
    
    if not db_path:
        print_error("Could not find metadata database (meta.sqlite)")
        print_info("Please run this script from the project root or backend directory")
        sys.exit(1)
    
    print_success(f"Found database at: {db_path}")
    return sqlite3.connect(str(db_path))


def list_users(conn: sqlite3.Connection) -> List[Tuple]:
    """Get all active users"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, email, role 
        FROM users 
        WHERE active = 1
        ORDER BY name
    """)
    return cursor.fetchall()


def list_datasources(conn: sqlite3.Connection, user_id: str) -> List[Tuple]:
    """Get all datasources owned by a user"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, type, created_at
        FROM datasources
        WHERE user_id = ? AND active = 1
        ORDER BY name
    """, (user_id,))
    return cursor.fetchall()


def list_dashboards(conn: sqlite3.Connection, user_id: str) -> List[Tuple]:
    """Get all dashboards owned by a user"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, name, created_at, updated_at
        FROM dashboards
        WHERE user_id = ?
        ORDER BY name
    """, (user_id,))
    return cursor.fetchall()


def get_datasource_shares(conn: sqlite3.Connection, datasource_id: str) -> List[Tuple]:
    """Get all shares for a datasource"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT ds.id, u.name, u.email, ds.permission
        FROM datasource_shares ds
        JOIN users u ON ds.user_id = u.id
        WHERE ds.datasource_id = ?
    """, (datasource_id,))
    return cursor.fetchall()


def get_dashboard_shares(conn: sqlite3.Connection, dashboard_id: str) -> List[Tuple]:
    """Get all shares for a dashboard"""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT sp.id, u.name, u.email, sp.permission
        FROM share_permissions sp
        JOIN users u ON sp.user_id = u.id
        WHERE sp.dashboard_id = ?
    """, (dashboard_id,))
    return cursor.fetchall()


def display_users(users: List[Tuple]) -> None:
    """Display users in a formatted table"""
    print(f"\n{Colors.BOLD}{'#':<4} {'Name':<30} {'Email':<40} {'Role':<10}{Colors.ENDC}")
    print(f"{'-'*4} {'-'*30} {'-'*40} {'-'*10}")
    
    for idx, (user_id, name, email, role) in enumerate(users, 1):
        role_colored = f"{Colors.WARNING}{role}{Colors.ENDC}" if role == "admin" else role
        print(f"{idx:<4} {name:<30} {email:<40} {role_colored:<10}")


def display_datasources(datasources: List[Tuple]) -> None:
    """Display datasources in a formatted table"""
    print(f"\n{Colors.BOLD}{'#':<4} {'Name':<40} {'Type':<15} {'Created':<20}{Colors.ENDC}")
    print(f"{'-'*4} {'-'*40} {'-'*15} {'-'*20}")
    
    for idx, (ds_id, name, ds_type, created_at) in enumerate(datasources, 1):
        # Format datetime
        created = created_at[:19] if created_at else "N/A"
        print(f"{idx:<4} {name:<40} {ds_type:<15} {created:<20}")


def display_dashboards(dashboards: List[Tuple]) -> None:
    """Display dashboards in a formatted table"""
    print(f"\n{Colors.BOLD}{'#':<4} {'Name':<50} {'Created':<20} {'Updated':<20}{Colors.ENDC}")
    print(f"{'-'*4} {'-'*50} {'-'*20} {'-'*20}")
    
    for idx, (dash_id, name, created_at, updated_at) in enumerate(dashboards, 1):
        # Format datetimes
        created = created_at[:19] if created_at else "N/A"
        updated = updated_at[:19] if updated_at else "N/A"
        print(f"{idx:<4} {name:<50} {created:<20} {updated:<20}")


def select_from_list(prompt: str, items: List, allow_multiple: bool = False, allow_all: bool = False) -> List[int]:
    """Generic selection function"""
    while True:
        if allow_all and allow_multiple:
            print(f"\n{prompt} (enter numbers separated by commas, or 'all'):")
        elif allow_multiple:
            print(f"\n{prompt} (enter numbers separated by commas):")
        else:
            print(f"\n{prompt}:")
        
        choice = input(f"{Colors.OKCYAN}> {Colors.ENDC}").strip()
        
        if allow_all and choice.lower() == 'all':
            return list(range(1, len(items) + 1))
        
        try:
            if allow_multiple:
                selections = [int(x.strip()) for x in choice.split(',')]
            else:
                selections = [int(choice)]
            
            # Validate all selections
            if all(1 <= sel <= len(items) for sel in selections):
                return selections
            else:
                print_error(f"Please enter numbers between 1 and {len(items)}")
        except ValueError:
            print_error("Invalid input. Please enter valid numbers.")


def confirm_action(prompt: str) -> bool:
    """Ask for confirmation"""
    while True:
        response = input(f"\n{Colors.WARNING}{prompt} (yes/no): {Colors.ENDC}").strip().lower()
        if response in ['yes', 'y']:
            return True
        elif response in ['no', 'n']:
            return False
        else:
            print_error("Please enter 'yes' or 'no'")


def transfer_datasources(
    conn: sqlite3.Connection,
    datasource_ids: List[str],
    target_user_id: str,
    target_user_name: str
) -> None:
    """Transfer datasources to new owner"""
    cursor = conn.cursor()
    
    for ds_id in datasource_ids:
        # Check if target user has a share for this datasource - remove it if so
        cursor.execute("""
            DELETE FROM datasource_shares 
            WHERE datasource_id = ? AND user_id = ?
        """, (ds_id, target_user_id))
        
        # Update ownership
        cursor.execute("""
            UPDATE datasources 
            SET user_id = ? 
            WHERE id = ?
        """, (target_user_id, ds_id))
    
    conn.commit()
    print_success(f"Successfully transferred {len(datasource_ids)} datasource(s) to {target_user_name}")


def transfer_dashboards(
    conn: sqlite3.Connection,
    dashboard_ids: List[str],
    target_user_id: str,
    target_user_name: str
) -> None:
    """Transfer dashboards to new owner"""
    cursor = conn.cursor()
    
    for dash_id in dashboard_ids:
        # Check if target user has a share for this dashboard - remove it if so
        cursor.execute("""
            DELETE FROM share_permissions 
            WHERE dashboard_id = ? AND user_id = ?
        """, (dash_id, target_user_id))
        
        # Update ownership
        cursor.execute("""
            UPDATE dashboards 
            SET user_id = ? 
            WHERE id = ?
        """, (target_user_id, dash_id))
    
    conn.commit()
    print_success(f"Successfully transferred {len(dashboard_ids)} dashboard(s) to {target_user_name}")


def main():
    """Main wizard function"""
    print_header("OWNERSHIP TRANSFER WIZARD")
    
    # Connect to database
    conn = get_connection()
    
    try:
        # Step 1: List all users
        print_header("Step 1: Select Source User (Current Owner)")
        users = list_users(conn)
        
        if not users:
            print_error("No users found in the database!")
            sys.exit(1)
        
        display_users(users)
        source_indices = select_from_list("Select source user", users, allow_multiple=False)
        source_idx = source_indices[0] - 1
        source_user_id, source_user_name, source_user_email, _ = users[source_idx]
        
        print_success(f"Selected source user: {source_user_name} ({source_user_email})")
        
        # Step 2: Choose what to transfer
        print_header("Step 2: Select Transfer Type")
        transfer_options = [
            ("datasources", "Datasources only"),
            ("dashboards", "Dashboards only"),
            ("both", "Both datasources and dashboards")
        ]
        
        print(f"\n{Colors.BOLD}What would you like to transfer?{Colors.ENDC}")
        for idx, (_, label) in enumerate(transfer_options, 1):
            print(f"  {idx}. {label}")
        
        transfer_choice = select_from_list("Select transfer type", transfer_options, allow_multiple=False)
        transfer_type = transfer_options[transfer_choice[0] - 1][0]
        print_success(f"Selected: {transfer_options[transfer_choice[0] - 1][1]}")
        
        # Step 3: Select items based on transfer type
        selected_datasources = []
        selected_dashboards = []
        
        if transfer_type in ["datasources", "both"]:
            print_header(f"Step 3{'a' if transfer_type == 'both' else ''}: Select Datasources to Transfer")
            datasources = list_datasources(conn, source_user_id)
            
            if not datasources:
                print_warning(f"{source_user_name} doesn't own any datasources!")
                if transfer_type == "datasources":
                    sys.exit(0)
            else:
                display_datasources(datasources)
                print_info(f"Total: {len(datasources)} datasource(s)")
                
                ds_indices = select_from_list(
                    "Select datasources to transfer",
                    datasources,
                    allow_multiple=True,
                    allow_all=True
                )
                
                selected_datasources = [datasources[i - 1] for i in ds_indices]
                print_success(f"Selected {len(selected_datasources)} datasource(s) for transfer")
        
        if transfer_type in ["dashboards", "both"]:
            step_label = "3b" if transfer_type == "both" else "3"
            print_header(f"Step {step_label}: Select Dashboards to Transfer")
            dashboards = list_dashboards(conn, source_user_id)
            
            if not dashboards:
                print_warning(f"{source_user_name} doesn't own any dashboards!")
                if transfer_type == "dashboards":
                    sys.exit(0)
            else:
                display_dashboards(dashboards)
                print_info(f"Total: {len(dashboards)} dashboard(s)")
                
                dash_indices = select_from_list(
                    "Select dashboards to transfer",
                    dashboards,
                    allow_multiple=True,
                    allow_all=True
                )
                
                selected_dashboards = [dashboards[i - 1] for i in dash_indices]
                print_success(f"Selected {len(selected_dashboards)} dashboard(s) for transfer")
        
        # Check if we have anything to transfer
        if not selected_datasources and not selected_dashboards:
            print_warning("No items selected for transfer!")
            sys.exit(0)
        
        # Step 4: Select target user
        print_header("Step 4: Select Target User (New Owner)")
        
        # Filter out source user from target selection
        target_users = [u for u in users if u[0] != source_user_id]
        
        if not target_users:
            print_error("No other users available for transfer!")
            sys.exit(1)
        
        display_users(target_users)
        target_indices = select_from_list("Select target user", target_users, allow_multiple=False)
        target_idx = target_indices[0] - 1
        target_user_id, target_user_name, target_user_email, _ = target_users[target_idx]
        
        print_success(f"Selected target user: {target_user_name} ({target_user_email})")
        
        # Step 5: Show summary and confirm
        print_header("Step 5: Confirm Transfer")
        print(f"\n{Colors.BOLD}Transfer Summary:{Colors.ENDC}")
        print(f"  From: {Colors.OKCYAN}{source_user_name}{Colors.ENDC} ({source_user_email})")
        print(f"  To:   {Colors.OKGREEN}{target_user_name}{Colors.ENDC} ({target_user_email})")
        
        if selected_datasources:
            print(f"\n{Colors.BOLD}Datasources to transfer:{Colors.ENDC}")
            for ds_id, ds_name, ds_type, _ in selected_datasources:
                print(f"  • {ds_name} ({ds_type})")
                
                # Check if there are existing shares
                shares = get_datasource_shares(conn, ds_id)
                if shares:
                    print(f"    {Colors.WARNING}Note: This datasource has {len(shares)} existing share(s){Colors.ENDC}")
        
        if selected_dashboards:
            print(f"\n{Colors.BOLD}Dashboards to transfer:{Colors.ENDC}")
            for dash_id, dash_name, _, _ in selected_dashboards:
                print(f"  • {dash_name}")
                
                # Check if there are existing shares
                shares = get_dashboard_shares(conn, dash_id)
                if shares:
                    print(f"    {Colors.WARNING}Note: This dashboard has {len(shares)} existing share(s){Colors.ENDC}")
        
        print_warning("\nThis action will:")
        total_items = len(selected_datasources) + len(selected_dashboards)
        if selected_datasources:
            print(f"  1. Change ownership of {len(selected_datasources)} datasource(s)")
        if selected_dashboards:
            num = 2 if selected_datasources else 1
            print(f"  {num}. Change ownership of {len(selected_dashboards)} dashboard(s)")
        print(f"  • Remove any existing shares the target user has for these items")
        print(f"  • Preserve all other shares")
        
        if not confirm_action("Do you want to proceed with this transfer?"):
            print_info("Transfer cancelled by user")
            sys.exit(0)
        
        # Step 6: Perform transfer
        print_header("Step 6: Transferring Ownership")
        
        if selected_datasources:
            datasource_ids = [ds[0] for ds in selected_datasources]
            transfer_datasources(conn, datasource_ids, target_user_id, target_user_name)
        
        if selected_dashboards:
            dashboard_ids = [dash[0] for dash in selected_dashboards]
            transfer_dashboards(conn, dashboard_ids, target_user_id, target_user_name)
        
        print_header("Transfer Complete!")
        total_items = len(selected_datasources) + len(selected_dashboards)
        
        if selected_datasources and selected_dashboards:
            print_success(f"Transferred {len(selected_datasources)} datasource(s) and {len(selected_dashboards)} dashboard(s)")
        elif selected_datasources:
            print_success(f"All {len(selected_datasources)} datasource(s) have been transferred successfully")
        else:
            print_success(f"All {len(selected_dashboards)} dashboard(s) have been transferred successfully")
        
        print_info(f"{target_user_name} is now the owner of the selected items")
        
    except KeyboardInterrupt:
        print(f"\n\n{Colors.WARNING}Operation cancelled by user{Colors.ENDC}")
        sys.exit(0)
    except Exception as e:
        print_error(f"An error occurred: {str(e)}")
        conn.rollback()
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
