import React from "react";
import { PublicKey } from "@solana/web3.js";
import { useAccountHistory } from "providers/accounts";
import { Slot } from "components/common/Slot";
import { displayTimestamp } from "utils/date";
import {
  useDetailedAccountHistory,
  useFetchDetailedAccountHistory,
} from "providers/accounts/detailed-history";
import { SlotRow } from "../TransactionHistoryCardWrapper";
import { Signature } from "components/common/Signature";
import { getTokenInstructionType } from "utils/instruction";
import { InstructionDetails } from "components/common/InstructionDetails";

export function TokenInstructionsDetails({
  pubkey,
  slotRows,
}: {
  pubkey: PublicKey;
  slotRows: SlotRow[];
}) {
  const address = pubkey.toBase58();
  const history = useAccountHistory(address);
  const fetchDetailedAccountHistory = useFetchDetailedAccountHistory(pubkey);
  const detailedHistoryMap = useDetailedAccountHistory(address);

  React.useEffect(() => {
    if (history?.data?.fetched) {
      fetchDetailedAccountHistory(history.data.fetched);
    }
  }, [history]); // eslint-disable-line react-hooks/exhaustive-deps

  const hasTimestamps = !!slotRows.find((element) => !!element.blockTime);
  const detailsList: React.ReactNode[] = [];

  slotRows.forEach(({ slot, signatureInfo, signature, blockTime, failed }) => {
    const parsed = detailedHistoryMap.get(signature);
    if (!parsed) return;

    const instructions = parsed.transaction.message.instructions;

    instructions.forEach((ix, index) => {
      const instructionType = getTokenInstructionType(
        parsed,
        ix,
        signatureInfo,
        index
      );

      if (instructionType) {
        detailsList.push(
          <tr
            key={signature + index}
            className={`${failed && "transaction-failed"}`}
            title={`${failed && "Transaction Failed"}`}
          >
            <td className="w-1">
              <Slot slot={slot} link />
            </td>

            <td>
              <InstructionDetails
                instructionType={instructionType}
                tx={signatureInfo}
              />
            </td>

            <td>
              <Signature signature={signature} link truncate />
            </td>

            {hasTimestamps && (
              <td className="text-muted">
                {blockTime ? displayTimestamp(blockTime * 1000, true) : "---"}
              </td>
            )}
          </tr>
        );
      }
    });
  });

  return (
    <div className="table-responsive mb-0">
      <table className="table table-sm table-nowrap card-table">
        <thead>
          <tr>
            <th className="text-muted w-1">Slot</th>
            <th className="text-muted">Instruction</th>
            <th className="text-muted">Transaction Signature</th>
            {hasTimestamps && <th className="text-muted w-auto">Timestamp</th>}
          </tr>
        </thead>
        <tbody className="list">{detailsList}</tbody>
      </table>
    </div>
  );
}
