import CampaignDetailsClient from "./CampaignDetailsClient";

export async function generateStaticParams() {
  const apiUrl = process.env.NEXT_PUBLIC_API_URL;
  const response = await fetch(`${apiUrl}/v3/campaigns/build/list`, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
    },
  });

  const campaigns = (await response.json()) as { _id: number }[];

  return campaigns.map((campaign) => ({
    id: campaign._id.toString(),
  }));
}

interface Props {
  params: Promise<{ id: string }>;
}

export default async function CampaignDetails({ params }: Props) {
  const { id } = await params;
  return <CampaignDetailsClient id={id} />;
}
