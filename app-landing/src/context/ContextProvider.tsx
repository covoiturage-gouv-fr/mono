'use client' 
import DashboardProvider from './DashboardProvider'; 

export function ContextProvider({children}: { children: React.ReactNode}) {
  return (
  <DashboardProvider>{children}</DashboardProvider>);
}

