'use client'
import { trimesterList } from '@/helpers/lists';
import { useMemo } from 'react';
import { useDashboardContext } from '../../context/DashboardProvider';
import SelectInList from '../common/SelectInList';


export default function SelectTrimester() {
  const { dashboard } = useDashboardContext();
  const trimesterAvailable = useMemo<{id: number; name: string; disabled: boolean;}[]>(()=>{
      const monthTrimester = [3,6,9,12];
      const list = trimesterList.map((m) => {
        if(new Date(dashboard.params.year, monthTrimester[m.id-1],1).getTime() > dashboard.lastPeriod) {
          return {...m, disabled: true}
        } else {
          return {...m, disabled: false}
        }
      })
      return list
  },[dashboard.params.year, dashboard.lastPeriod]);
  return (
    <>
      <SelectInList
        labelId='trimester'
        label='Trimestre'
        id={dashboard.params.trimester}
        list={trimesterAvailable}
        sx={{ minWidth: 120, margin: '0 0.5em' }}
        onChange={(v) => dashboard.onChangeTrimester(v)}
      />
    </>
  );
}
