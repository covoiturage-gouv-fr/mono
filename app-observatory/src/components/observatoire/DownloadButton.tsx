import Button from '@codegouvfr/react-dsfr/Button';
import { downloadData } from '@/helpers/map';
import { FeatureCollection } from 'geojson';

interface DownloadButtonProps<T extends object> {
  title: string,
  data: T[] | FeatureCollection,
  filename: string,
  type?: 'geojson' | 'csv',
}
export default function DownloadButton<T extends object>(props:DownloadButtonProps<T>) {
  return(
    <Button onClick={() => downloadData(props.filename, props.data, props.type ?? 'csv')}
      iconId="fr-icon-download-fill"
      iconPosition="right"
    >
     {props.title}
    </Button>
  );
}