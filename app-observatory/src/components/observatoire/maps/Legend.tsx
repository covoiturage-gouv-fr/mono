import { LegendInterface } from '@/interfaces/observatoire/componentsInterfaces';
import style from './Legend.module.scss';
import { CSSProperties } from 'react';

export default function Legend(props: LegendInterface) {
  return (
    <div className={style.legend} style={props.order && props.order > 1 ? {"bottom":`${props.order*90}px`} : {} as CSSProperties} >
      <div className={style.legend_title}>{props.title}</div>
      {props.type !== 'proportional_circles' &&
        props.classes.map((c,i) =>
        <div key={i} className={style.item}>
          <span className={style.legend_class} style={{"height":`${c.width}px`,"backgroundColor":`rgb(${c.color[0]},${c.color[1]},${c.color[2]})`} as CSSProperties}></span>
          <span className={style.legend_classname}>{c.val}</span>
        </div>
      )}
      {props.type == 'proportional_circles' &&
       <div className={style.proportional_circles} style={{"minHeight":`${props.classes[0].width*2}px`} as CSSProperties}>
          {props.classes.map((c,i) =>
            <div key={i} className={style.item}>
              <div className={style.circle_class} style={{
                "height":`${c.width*2}px`,
                "width":`${c.width*2}px`,
                "bottom":'0px',
                "left": `${props.classes[0].width-c.width}px`,
                "backgroundColor":`rgb(${c.color[0]},${c.color[1]},${c.color[2]})`
              } as CSSProperties}></div>
              <hr style={{
                "width":`${props.classes[0].width}px`,
                "bottom":`${c.width*2-1}px`,
                "left": `${props.classes[0].width}px`
              } as CSSProperties} />
              <div className={style.circle_label} style={{
                "bottom":`${c.width*2-10}px`,
                "left": `${props.classes[0].width*2+10}px`
              } as CSSProperties}>{c.val}</div>
            </div>
          )}
        </div>
      }
    </div>
  );
}
