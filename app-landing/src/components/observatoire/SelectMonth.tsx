'use client'
import { monthList } from '@/helpers/lists';
import { useEffect, useState } from 'react';
import { useDashboardContext } from '../../context/DashboardProvider';
import SelectInList from '../common/SelectInList';


export default function SelectMonth() {
  const { dashboard } = useDashboardContext();
 
  const [monthAvailable, setMonthAvailable] = useState<{id: number; name: string; disabled: boolean;}[]>([]);
  useEffect(()=>{
      const list = monthList.map((m) => {
        if(new Date(dashboard.params.year, m.id).getTime() > dashboard.lastPeriod) {
          return {...m, disabled: true}
        } else {
          return {...m, disabled: false}
        }
      })
      setMonthAvailable(list)
  },[dashboard.params.year, dashboard.lastPeriod]);
  return (
    <>
      <SelectInList
        labelId='mois'
        label='Mois'
        id={dashboard.params.month}
        list={monthAvailable}
        sx={{ minWidth: 120, margin: '0 1em' }}
        onChange={(v) => dashboard.onChangeMonth(v)}
      />
    </>
  );
}
