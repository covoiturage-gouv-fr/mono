import DownloadButton from '@/components/observatoire/DownloadButton';
import { GetApiUrl } from '@/helpers/api';
import { useApi } from '@/hooks/useApi';
import { Hour } from '@/interfaces/observatoire/Perimeter';
import { DistributionHoraireDataInterface } from '@/interfaces/observatoire/dataInterfaces';
import { fr } from '@codegouvfr/react-dsfr';
import { BarElement, CategoryScale, ChartData, Chart as ChartJS, Legend, LinearScale, Title, Tooltip } from 'chart.js';
import { Bar } from 'react-chartjs-2';
import { useDashboardContext } from '../../../../context/DashboardProvider';

ChartJS.register(BarElement, CategoryScale, LinearScale, Title, Tooltip, Legend);

export default function RepartitionHoraireGraph({ title }: { title: string }) {
  const { dashboard } = useDashboardContext();
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins:{
      legend: {
        display: dashboard.params.type !== 'country' ? true : false
      }
    },
  };

  const params = [
    `code=${dashboard.params.code}`,
    `type=${dashboard.params.type}`,
    `year=${dashboard.params.year}`,
  ];
  const url = GetApiUrl('journeys-by-hours', params);
  const { data, error, loading } = useApi<DistributionHoraireDataInterface[]>(url);

  const dataWithNull = (data:Hour[]) => {
    const arrayA = [...data]
    const arrayB:number[] = Array.from({length:24}, (v, k) => k);
    const lookupA = arrayA.reduce<Hour[]>((acc, curr) => ({ ...acc, [curr.hour]: curr }), {} as Hour[]);
    const result = arrayB.map((h) => lookupA[h] ?? { h, journeys: 0 });
    return result
  }

  const chartData = () => {
    const labels = ['0h','1h','2h','3h','4h','5h','6h','7h','8h','9h','10h','11h','12h','13h','14h','15h','16h','17h','18h','19h','20h','21h','22h','23h'];
    const datasets = [
      {
        label: 'Tout sens confondus',
        data: data ? dataWithNull(data.find((d) => d.direction === 'both')?.hours || []).map((d) => d.journeys) : [],
        borderColor:'#000091',
        backgroundColor:'rgba(106, 106, 244, 0.8)',
        tension: 0.1,
        datalabels: {
          labels: {
            title: null
          },
        },
      }
    ];
    dashboard.params.type !== 'country' ? datasets.splice(0,1) && datasets.push(
      {
        label: 'Origine',
        data: data ? dataWithNull(data.find((d) => d.direction === 'from')?.hours || []).map((d) => d.journeys) : [],
        borderColor:'#000091',
        backgroundColor:'rgba(106, 106, 244, 0.8)',
        tension: 0.1,
        datalabels: {
          labels: {
            title: null
          },
        },
      },
      {
        label: 'Destination',
        data: data ? dataWithNull(data.find((d) => d.direction === 'to')?.hours || []).map((d) => d.journeys) : [],
        borderColor:'#000091',
        backgroundColor:'rgba(183, 167, 63, 0.8)',
        tension: 0.1,
        datalabels: {
          labels: {
            title: null
          },
        },
      }
    ) : '';
    return { labels: labels, datasets: datasets };
  };

  return (
    <>
      {loading && (
        <div className={fr.cx('fr-callout')}>
          <h3 className={fr.cx('fr-callout__title')}>{title}</h3>
          <div>Chargement en cours...</div>
        </div>
      )}
      {error && (
        <div className={fr.cx('fr-callout')}>
          <h3 className={fr.cx('fr-callout__title')}>{title}</h3>
          <div>{`Un problème est survenu au chargement des données: ${error}`}</div>
        </div>
      )}
      {!data || data.length == 0 && (
        <div className={fr.cx('fr-callout')}>
          <h3 className={fr.cx('fr-callout__title')}>{title}</h3>
          <div>Pas de données disponibles pour ce graphique...</div>
        </div>
      )}
      {!loading && !error && data && data.length > 0 && (
        <div className={fr.cx('fr-callout')}>
          <div className={fr.cx('fr-callout__title','fr-text--xl')}>
            {title}  
          </div>
          <DownloadButton 
            title={'Télécharger les données du graphique'}
            data={data!}
            filename='repartition_horaire'
          />
          <figure className='graph-wrapper' style={{ backgroundColor: '#fff', height:"350px" }}>
            <Bar options={options} data={chartData() as ChartData<"bar",number[]>} aria-hidden />
            { chartData() &&
              <figcaption className={fr.cx('fr-sr-only')}>
                {chartData().datasets[0] &&
                  <>
                    <p>{'Données concernant les horaires à l\'origine des trajets'}</p>
                    <ul>
                      { chartData().datasets[0].data.map((d,i) =>{
                        return (
                          <li key={i}>{chartData().labels[i]} : {d} trajets</li>
                        )
                      })} 
                    </ul>
                  </> 
                }
                {chartData().datasets[1] &&
                  <>
                    <p>Données concernant les horaires à destination des trajets</p>
                    <ul>
                      { chartData().datasets[1].data.map((d,i) =>{
                        return (
                          <li key={i}>{chartData().labels[i]} : {d} trajets</li>
                        )
                      })} 
                    </ul>
                  </> 
                }
              </figcaption>
            }
          </figure>
        </div>
      )}
    </>
  );
}
