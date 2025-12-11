'use client'
import { yearList } from '@/helpers/lists';
import { useDashboardContext } from '../../context/DashboardProvider';
import SelectInList from '../common/SelectInList';


export default function SelectYear() {
  const { dashboard } = useDashboardContext();
 
  return (
    <>
      <SelectInList
        labelId='year'
        label='Année'
        id={dashboard.params.year}
        list={yearList}
        sx={{ minWidth: 120, margin: '0 0.5em' }}
        onChange={(v) => dashboard.onChangeYear(v)}
      />
    </>
  );
}
