import React, {useEffect, useState} from 'react'
import './App.css';

export function getLastTemps() {
  return fetch('http://localhost:5000/api/pixels')
    .then(data => data.json()); //['pixels'])
}


function App() {
  const [lastTemps, setLastTemps] = useState({});

  useEffect(() => {
    let mounted = true;
    getLastTemps()
      .then(temps => {
        if(mounted) {
          setLastTemps(temps)
          console.log("lastTemps just got set: "+JSON.stringify(temps));
          //console.log("keys="+Object.keys(temps))
        }
      })
    return () => mounted = false;
  }, [])

  return (
    <div className="App">
      <header className="App-header">
       WServ - Wiliot Challenge<br/>
       {console.log("lastTemps="+lastTemps)}
       <ol key="latest_temps_list">
       {
        (() => {
            if ((lastTemps) && (lastTemps["pixels"])) {
              //lastTemps.map(t => <li key={t.item}>{t.item}</li>)
              return Object.entries(lastTemps["pixels"]).map( ([key, value]) => <li>{new Date(value["time"]*1).toLocaleString()} - {value["pixelname"]}={Number(value["temp"]).toFixed(2)}&deg;C </li> )
              //console.log("rendering "+JSON.stringify(lastTemps["pixels"]))
              //return JSON.stringify(lastTemps["pixels"])
            }
        })()
       }
       </ol>
       {//lastTemps.map(t => <li key={t.item}>{t.item}</li>)
       }

      </header>
    </div>
  );
}

export default App;
