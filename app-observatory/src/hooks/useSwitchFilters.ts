import { useCallback, useState } from 'react';
import { SwitchFilterInterface } from '../interfaces/observatoire/helpersInterfaces';


export const useSwitchFilters = (props:SwitchFilterInterface[]) => {
  const [switchFilters, setSwitchFilters] = useState(props);
  
  const onChangeSwitchFilter = useCallback((value: SwitchFilterInterface) => {
    setSwitchFilters( f =>{
      const id = f.findIndex((obj => obj.name == value.name));
      f[id] = value
      return [...f]
    });
  },[]);
  return { switchFilters, onChangeSwitchFilter };
};