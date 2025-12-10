import DownloadButton from '@/components/observatoire/DownloadButton';
import { GetApiUrl } from '@/helpers/api';
import { useApi } from '@/hooks/useApi';
import { IncentiveDataInterface } from '@/interfaces/observatoire/dataInterfaces';
import { fr } from '@codegouvfr/react-dsfr';
import { ArcElement, ChartData, Chart as ChartJS, Legend, Title, Tooltip } from 'chart.js';
import ChartDataLabels, { Context } from 'chartjs-plugin-datalabels';
import { Doughnut } from 'react-chartjs-2';
import { useDashboardContext } from '../../../../context/DashboardProvider';

ChartJS.register(ArcElement, Title, Tooltip, Legend);

export default function IncentiveGraph({ title }: { title: string }) {
  const { dashboard } = useDashboardContext();
  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
      },
    },
  };
  const params = [
    `code=${dashboard.params.code}`,
    `type=${dashboard.params.type}`,
    `year=${dashboard.params.year}`,
    `direction=both`
  ];
  const url = GetApiUrl('incentive', params);
  const { data, error, loading } = useApi<IncentiveDataInterface[]>(url);
  const plugins: any = [ChartDataLabels];
  const dataset = [data && data[0] ? data[0].collectivite : 0, data && data[0] ? data[0].operateur: 0, data && data[0] ? data[0].autres : 0];
  const datasetSum = (dataset: number[]) => {
    let sum = 0;
    dataset.map((d) => {
      sum += d;
    });
    return sum;
  };
  const chartData = () => {
    const labels = ['Collectivité(s)', 'Opérateur(s)', 'Autres',];
    const datasets = [
      {
        label: 'incitations',
        data: dataset,
        backgroundColor: ['#3182bd', '#6baed6', '#9ecae1'],
        datalabels: {
          labels: {
            name: {
              align: 'middle',
              font: (context: Context) => {
                const avgSize = Math.round((context.chart.height + context.chart.width) / 2);
                const params = {
                  size: Math.round(avgSize / 24) > 10 ? 14 : Math.round(avgSize / 24),
                  weight: 'bold',
                };
                return params;
              },
              color: 'black',
              formatter: (value: number, ctx: Context) => {
                return ctx.chart.data.labels ? ctx.chart.data.labels[ctx.dataIndex] : '';
              },
            },
            value: {
              align: 'bottom',
              color: 'black',
              font: (context: Context) => {
                const avgSize = Math.round((context.chart.height + context.chart.width) / 2);
                const params = {
                  size: Math.round(avgSize / 24) > 10 ? 12 : Math.round(avgSize / 24),
                };
                return params;
              },
              formatter: (value: number, ctx: Context) => {
                const dataArr = ctx.chart.data.datasets[0].data;
                const total = dataArr.reduce((acc: number, val) => acc + Number(val), 0)
                const percentage = ((value / total) * 100).toFixed(1)  + '%';
                return percentage;
              },
            },
          },
        },
      }
    ];
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
            filename='repartition_distance'
          />
          <figure className='graph-wrapper' style={{ backgroundColor: '#fff', height:"350px"}}>
            <Doughnut options={options} plugins={plugins} data={chartData() as ChartData<"doughnut",number[]>} aria-hidden />
            { chartData() &&
              <figcaption className={fr.cx('fr-sr-only')}>
                {dataset &&
                  <>
                    <p>{'Données de répartition des trajets (tout sens confondus)'}</p>
                    <ul>
                      { dataset.map((d,i) =>{
                        return (
                          <li key={i}>{chartData().labels[i]} : {((d * 100) / datasetSum(dataset)).toFixed(1) + '%'}</li>
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
