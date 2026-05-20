'use client';

import { useIsDark } from '@codegouvfr/react-dsfr/useIsDark';
import { useEffect } from 'react';

const BASE = 'https://jedonnemonavis.numerique.gouv.fr';

export default function JDMA() {
  const { isDark } = useIsDark();

  useEffect(() => {
    document.querySelector('script[data-jdma-form-url]')?.remove();

    const script = document.createElement('script');
    script.src = `${BASE}/static/jdma-modal-widget.js`;
    script.dataset.jdmaFormUrl = `${BASE}/Demarches/avis/2246?button=4684`;
    script.dataset.jdmaButtonImage = `${BASE}/static/buttons/button-feedback-solid-${isDark ? 'dark' : 'light'}.svg`;
    script.dataset.jdmaButtonLabel = 'Faire un retour';
    script.dataset.jdmaPosition = 'bottom-right';
    script.defer = true;
    document.body.appendChild(script);

    return () => script.remove();
  }, [isDark]);

  return null;
}
