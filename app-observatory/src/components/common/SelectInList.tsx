import { FormControl, InputLabel, MenuItem, Select, SxProps } from '@mui/material';

interface Props {
  labelId:string;
  label: string;
  id: number;
  list: {
    id: number;
    name: string;
    disabled?: boolean;
  }[];
  sx?: SxProps;
  onChange: (v: number) => void;
}

export default function SelectInList(props: Props) {
  return (
    <>
      <FormControl sx={props.sx}>
        <InputLabel id={props.labelId}>{props.label}</InputLabel>
        <Select
         labelId={props.labelId} 
          value={props.id}
          label={props.label}
          onChange={(event) => props.onChange(event.target.value as number)}
        >
          {props.list.map((d) => (
            <MenuItem key={d.id} disabled={d.disabled} value={d.id}>
              {d.name}
            </MenuItem>
          ))}
        </Select>
      </FormControl>
    </>
  );
}
