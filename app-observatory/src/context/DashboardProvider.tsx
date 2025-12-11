import { useDashboard } from '@/hooks/useDashboard';
import { DashboardContextType } from '@/interfaces/common/contextInterface';
import { createContext, useContext } from 'react';
 

const DashboardContext = createContext<DashboardContextType | undefined>(undefined);

export default function DashboardContextProvider({children}: { children: React.ReactNode}) {
  const dashboard = useDashboard();
  return( 
    <DashboardContext.Provider value={{dashboard}}>
      {children}
    </DashboardContext.Provider>
  )
}

export const useDashboardContext = () => {
  const context = useContext(DashboardContext);
  if (!context) {
    throw new Error('useDashboardContext must be used within an DashboardContextProvider');
  }
  return context;
};
