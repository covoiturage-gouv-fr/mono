import { ToggleSwitch } from "@codegouvfr/react-dsfr/ToggleSwitch";
import { useSwitchFilters } from '@/hooks/useSwitchFilters';
import { SwitchFilterInterface } from '../../../interfaces/observatoire/helpersInterfaces';

type SwitchFilterProps = {
  filters: SwitchFilterInterface[];
  labelPosition?: 'left' | 'right';
  showCheckedHint?: boolean;
};

export default function SwitchFilters(props: SwitchFilterProps) {
  const { switchFilters, onChangeSwitchFilter } = useSwitchFilters(props.filters)
  
  return (
    <>
    {switchFilters.map((s,i) =>(
      <ToggleSwitch 
        key={i}
        label={s.name}
        labelPosition={props.labelPosition}
        showCheckedHint={props.showCheckedHint}
        checked={s.active}
        onChange={checked => onChangeSwitchFilter({name: s.name, active: checked})}
      />
    ))}
    
    </>
  );
};