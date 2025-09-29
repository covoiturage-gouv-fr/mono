"use client";
import { Tabs } from "@codegouvfr/react-dsfr/Tabs";
import { useState } from "react";
import TabCampaigns from "./TabCampaigns";
import TabExport from "./TabExport";

export default function TabsNav() {
  const [activeTab, setActiveTab] = useState("1");
  const tabs = [
    {
      tabId: "1",
      label: "Export des données",
    },
    {
      tabId: "2",
      label: `Suivi des campagnes`,
    },
  ];
  const tabContent = () => {
    switch (activeTab) {
      case "1":
        return <TabExport />;
      case "2":
        return <TabCampaigns />;
    }
  };

  return (
    <Tabs tabs={tabs} selectedTabId={activeTab} onTabChange={setActiveTab}>
      {tabContent()}
    </Tabs>
  );
}
