import Button from '@codegouvfr/react-dsfr/Button';
import { downloadData } from '@/helpers/map';
import { FeatureCollection } from 'geojson';

interface DownloadButtonProps {
  title: string,
  data: any[] | FeatureCollection,
  filename: string,
  type?: 'geojson' | 'csv',
}
export default function DownloadButton(props:DownloadButtonProps) {
  return(
    <Button onClick={() => downloadData(props.filename, props.data, props.type ?? 'csv')}
      iconId="fr-icon-download-fill"
      iconPosition="right"
    >
     {props.title}
    </Button>
  );
}