import { periodList } from '@/helpers/lists';
import { PeriodType } from '@/interfaces/observatoire/componentsInterfaces';
import { FormControl, InputLabel, MenuItem, Select } from '@mui/material';
import { useDashboardContext } from '../../context/DashboardProvider';

type SelectPeriodProps = {
  id: string,
  label: string,
};

export default function SelectPeriod(props: SelectPeriodProps) {
  const { dashboard } = useDashboardContext();
  return (
    <>
      <FormControl sx={{ minWidth: 200, margin: '0 0.5em' }}>
        <InputLabel id={props.id}>{props.label}</InputLabel>
        <Select
          labelId={props.id}
          value={dashboard.params.period}
          label={props.label}
          onChange={(event) => dashboard.onChangePeriod(event.target.value as PeriodType)}
        >
          {periodList.map((d, i: number) => (
            <MenuItem key={i} value={d.id}>
              {d.name}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
    </>
  );
}
