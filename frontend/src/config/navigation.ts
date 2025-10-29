export type SidebarItem = { type: 'item'; label: string; description?: string; href?: string }
export type SidebarGroup = { type: 'group'; label: string; position?: 'top' | 'bottom'; items: SidebarItem[] }
export type SidebarConfig = { sidebar: Array<SidebarItem | SidebarGroup> }

// Route map for sidebar labels -> hrefs
const routes: Record<string, string> = {
  'Home': '/home',
  'Build New Dashboard': '/builder',
  'My Dashboards': '/dashboards/mine',
  'Shared With Me': '/dashboards/shared',
  'Add Datasource': '/datasources/sources?add=1',
  'My Datasources': '/datasources/sources',
  'Data Model': '/datasources/data-model',
  'Alerts & Notifications': '/alerts',
  'Contacts Manager': '/contacts',
  'Change Password': '/users/change-password',
  'Logout': '/logout',
}

export const navConfig: SidebarConfig = {
  sidebar: [
    { type: 'item', label: 'Home', description: 'Previews of user\'s dashboards; allows add, publish/unpublish, edit, delete dashboards.', href: routes['Home'] },
    {
      type: 'group',
      label: 'Dashboards',
      items: [
        { type: 'item', label: 'Build New Dashboard', description: 'Create a new dashboard from scratch.', href: routes['Build New Dashboard'] },
        { type: 'item', label: 'My Dashboards', description: 'List of dashboards created by the user.', href: routes['My Dashboards'] },
        { type: 'item', label: 'Shared With Me', description: 'Dashboards shared by others; user can \'Add to Collection\'.', href: routes['Shared With Me'] },
      ],
    },
    {
      type: 'group',
      label: 'Datasources',
      items: [
        { type: 'item', label: 'Add Datasource', description: 'Connect a new datasource.', href: routes['Add Datasource'] },
        { type: 'item', label: 'My Datasources', description: 'Manage datasources owned by the user.', href: routes['My Datasources'] },
        { type: 'item', label: 'Data Model', description: 'Manage local DuckDB tables, custom columns and transforms.', href: routes['Data Model'] },
      ],
    },
    {
      type: 'group',
      label: 'Tools',
      items: [
        { type: 'item', label: 'Alerts & Notifications', description: 'Create, view, and manage alert rules; configure email/SMS providers.', href: routes['Alerts & Notifications'] },
        { type: 'item', label: 'Contacts Manager', description: 'Manage contacts; import/export; bulk email/SMS.', href: routes['Contacts Manager'] },
      ],
    },
    {
      type: 'group',
      label: 'Profile',
      position: 'bottom',
      items: [
        { type: 'item', label: 'Change Password', description: 'Update account password.', href: routes['Change Password'] },
        { type: 'item', label: 'Logout', description: 'Sign out of the application.', href: routes['Logout'] },
      ],
    },
  ],
}

export function resolveHref(label: string): string | undefined {
  return routes[label]
}
