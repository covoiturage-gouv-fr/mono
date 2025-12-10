'use client'
import { semesterList } from '@/helpers/lists';
import { useEffect, useState } from 'react';
import { useDashboardContext } from '../../context/DashboardProvider';
import SelectInList from '../common/SelectInList';


export default function SelectSemester() {
  const { dashboard } = useDashboardContext();
  const [semesterAvailable, setSemesterAvailable] = useState<{id: number; name: string; disabled: boolean;}[]>([]);
  useEffect(()=>{
      const monthSemester = [6,12];
      const list = semesterList.map((m) => {
        if(new Date(dashboard.params.year, monthSemester[m.id-1],1).getTime() > dashboard.lastPeriod) {
          return {...m, disabled: true}
        } else {
          return {...m, disabled: false}
        }
      })
      setSemesterAvailable(list)
  },[dashboard.params.year, dashboard.lastPeriod]);
  return (
    <>
      <SelectInList
        labelId='semester'
        label='Semestre'
        id={dashboard.params.semester}
        list={semesterAvailable}
        sx={{ minWidth: 120, margin: '0 0.5em' }}
        onChange={(v) => dashboard.onChangeSemester(v)}
      />
    </>
  );
}
